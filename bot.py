import os
import discord
from discord.ext import commands, tasks
import asyncpg
from dotenv import load_dotenv
from datetime import timezone, timedelta
from datetime import datetime, time, timedelta
import logging
import asyncio
from difflib import SequenceMatcher
import random
from logging.handlers import RotatingFileHandler
import pytz
import functools
import sqlparse


# Setup
logging.basicConfig(level=logging.INFO)
load_dotenv()

async def setup():
    await create_db_pool()
    await ensure_tables_exist()
    update_leaderboard.start()
    daily_challenge.start()
    challenge_time_over.start()
    update_weekly_heroes.start()
    check_scheduled_posts.start()
    # Any other initialization tasks...

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents, help_command=None)

class ThreadSafeDict:
    def __init__(self):
        self._dict = {}
        self._lock = asyncio.Lock()

    async def get(self, key, default=None):
        async with self._lock:
            return self._dict.get(key, default)

    async def set(self, key, value):
        async with self._lock:
            self._dict[key] = value

    async def pop(self, key, default=None):
        async with self._lock:
            return self._dict.pop(key, default)

    async def __contains__(self, key):
        async with self._lock:
            return key in self._dict

# Replace your global dictionaries with these thread-safe versions
user_questions = ThreadSafeDict()
user_attempts = ThreadSafeDict()
user_skips = ThreadSafeDict()
user_last_active = ThreadSafeDict()

CHANNEL_IDS = [int(id.strip()) for id in os.getenv('CHANNEL_ID', '').split(',') if id.strip()]

DB_SEMAPHORE = asyncio.Semaphore(10)  # Increase from 5 to 10 or higher if needed

# Add these near the top of your file, with other global variables
user_timers = {}

# Add this near the top of your file with other global variables
ADMIN_IDS = [1235457227733864469]  # Admin user ID

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def create_discord_table(headers, data):
    table = "| " + " | ".join(headers) + " |\n"
    table += "|" + "|".join(["---" for _ in headers]) + "|\n"
    for row in data:
        table += "| " + " | ".join(str(item) for item in row) + " |\n"
    return f"```\n{table}\n```"

async def create_db_pool():
    try:
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            raise ValueError("DATABASE_URL is not set in the environment variables")
        
        logging.info("Attempting to connect to database using DATABASE_URL")
        
        bot.db = await asyncpg.create_pool(database_url, min_size=1, max_size=10, ssl='require')
        logging.info("Database connection pool established")
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        raise

def retry_on_failure(max_retries=3, delay=1):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except (asyncpg.InterfaceError, asyncio.TimeoutError) as e:
                    if attempt == max_retries - 1:
                        raise
                    logging.warning(f"Database operation failed, retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
            return await func(*args, **kwargs)
        return wrapper
    return decorator

@retry_on_failure()
async def ensure_user_exists(user_id, username):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                await conn.execute('''
                    INSERT INTO users (user_id, username)
                    VALUES ($1, $2)
                    ON CONFLICT (user_id) DO UPDATE SET username = $2
                ''', user_id, username)
    except Exception as e:
        logging.error(f"Error ensuring user exists: {e}")
        raise

async def get_user_stats(user_id):
    async with DB_SEMAPHORE:
        async with bot.db.acquire() as conn:
            stats = await conn.fetchrow('''
                SELECT 
                    COUNT(*) as total_answers,
                    SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) as correct_answers,
                    SUM(points) as total_points
                FROM user_submissions
                WHERE user_id = $1
            ''', user_id)
    return stats

async def get_question(difficulty=None, user_id=None, topic=None, company=None):
    try:
        logging.info(f"Fetching question for user_id: {user_id}, difficulty: {difficulty}, topic: {topic}, company: {company}")
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                query = '''
                    SELECT q.* FROM questions q
                    LEFT JOIN user_submissions us ON q.id = us.question_id AND us.user_id = $1
                    WHERE (us.question_id IS NULL OR us.is_correct = FALSE)
                    AND ($2::VARCHAR IS NULL OR q.difficulty = $2::VARCHAR)
                    AND ($3::VARCHAR IS NULL OR q.topic = $3::VARCHAR)
                    AND ($4::VARCHAR IS NULL OR q.company = $4::VARCHAR)
                    ORDER BY RANDOM() LIMIT 1
                '''
                logging.info(f"Executing query: {query}")
                question = await conn.fetchrow(query, user_id, difficulty, topic, company)
        logging.info(f"Fetched question: {question}")
        return question
    except Exception as e:
        logging.error(f"Error fetching question: {e}")
        return None

async def get_week_start():
    ist_now = get_ist_time()
    return ist_now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=ist_now.weekday())

async def update_weekly_points(user_id, points):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                week_start = await get_week_start()
                await conn.execute('''
                    INSERT INTO weekly_points (user_id, points, week_start)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (user_id, week_start)
                    DO UPDATE SET points = weekly_points.points + EXCLUDED.points
                ''', user_id, points, week_start)
    except Exception as e:
        logging.error(f"Error updating weekly points: {e}")
        raise

async def get_weekly_points(user_id):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                week_start = await get_week_start()
                points = await conn.fetchval('''
                    SELECT COALESCE(points, 0)
                    FROM weekly_points
                    WHERE user_id = $1 AND week_start = $2
                ''', user_id, week_start)
        return points
    except Exception as e:
        logging.error(f"Error getting weekly points: {e}")
        return 0

async def get_daily_points(user_id, date):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                points = await conn.fetchval('''
                    SELECT COALESCE(points, 0)
                    FROM daily_points
                    WHERE user_id = $1 AND date = $2
                ''', user_id, date)
        return points or 0  # Return 0 if points is None
    except Exception as e:
        logging.error(f"Error getting daily points: {e}")
        return 0

async def update_daily_points(user_id, date, points):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                await conn.execute('''
                    INSERT INTO daily_points (user_id, date, points)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (user_id, date)
                    DO UPDATE SET points = daily_points.points + EXCLUDED.points
                ''', user_id, date, points)
    except Exception as e:
        logging.error(f"Error updating daily points: {e}")
        raise

async def display_question(ctx, question):
    difficulty = question['difficulty'].capitalize()
    points = {'easy': 60, 'medium': 80, 'hard': 120}.get(question['difficulty'], 0)
    time_limit = {'easy': 10, 'medium': 15, 'hard': 25}.get(question['difficulty'], 10)
    
    user_id = ctx.author.id
    max_attempts = await get_max_attempts(user_id, question['id'])
    
    message = f"Question ID: {question['id']}\n"
    message += f"Difficulty: {difficulty} ({points} points)\n"
    message += f"Time Limit: {time_limit} minutes ⏳\n"
    message += f"Attempts Remaining: {max_attempts}\n"
    if question.get('topic'):
        message += f"Topic: {question['topic']}\n"
    if question.get('company'):
        message += f"Company: {question['company']}\n"
    message += f"\n{question['question']}\n"
    if question['datasets']:
        message += f"\nDataset:\n```\n{question['datasets']}\n```"
    message += "\nUse `!submit` followed by your SQL query to answer!"
    message += "\nUse `!skip` if you want to try a different question.\n"
    message += f"\nUse Create Table in site for free \![Click Here To Create Table](https://zeroanalyst.com/sql/)"
    message += "\nYou can come back here and submit your code here! using `!submit` command"
    
    await ctx.send(message)
    
    # Start the timer
    if user_id in user_timers:
        user_timers[user_id].cancel()
    user_timers[user_id] = asyncio.create_task(question_timer(ctx, question['id'], time_limit))

async def check_daily_limit(ctx, user_id):
    today = get_ist_time().date()
    daily_points = await get_daily_points(user_id, today)
    daily_submissions = await get_daily_submissions(user_id, today)
    logging.info(f"User {user_id} daily points: {daily_points}, daily submissions: {daily_submissions}")
    if daily_points <= -50 or daily_submissions >= 10:
        await ctx.send("You've reached the daily limit. Please try again tomorrow! 🌙")
        return False
    return True

async def get_daily_submissions(user_id, date):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                submissions = await conn.fetchval('''
                    SELECT COUNT(*) FROM user_submissions
                    WHERE user_id = $1 AND DATE(submitted_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') = $2
                ''', user_id, date)
        return submissions
    except Exception as e:
        logging.error(f"Error getting daily submissions: {e}")
        return 0

def db_connection_required():
    async def predicate(ctx):
        if not hasattr(bot, 'db'):
            await ctx.send("Database connection not available. Please try again later.")
            return False
        return True
    return commands.check(predicate)

@bot.command(name='sql')
@commands.cooldown(1, 60, commands.BucketType.user)
async def sql(ctx):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)

    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                preference = await conn.fetchval('''
                    SELECT preferred_difficulty FROM user_preferences
                    WHERE user_id = $1
                ''', user_id)
        
        question = await get_question(difficulty=preference, user_id=user_id)
        if question:
            await user_questions.set(user_id, question)
            await user_attempts.set(user_id, 0)
            await display_question(ctx, question)
        else:
            await ctx.send("Sorry, no questions available at your preferred difficulty. Try `!reset_preference` to see questions from all difficulties, or use `!topic <topic>` to try a specific topic.")
    except Exception as e:
        logging.error(f"Error in sql command: {e}")
        await ctx.send("An error occurred while fetching a question. Please try again later.")

@sql.error
async def sql_error(ctx, error):
    if isinstance(error, commands.CommandOnCooldown):
        await ctx.send(f"Whoa there, eager learner! You can try another question in {error.retry_after:.2f} seconds. Take a moment to review your last query or check out your stats with `!my_stats`.")
    else:
        logging.error(f"Unhandled error in sql command: {error}")
        # No need to send an error message to the user here

@bot.command()
async def submit(ctx, *, answer):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    question = await user_questions.get(user_id)
    if question:
        current_attempts = await user_attempts.get(user_id, 0)
        await user_attempts.set(user_id, current_attempts + 1)
        await process_answer(ctx, user_id, answer)
    else:
        await ctx.send("You don't have an active question. Use `!sql` to get a new question.")

async def process_answer(ctx, user_id, answer):
    question = await user_questions.get(user_id)
    if not question:
        await ctx.send("You don't have an active question. Use `!sql` to get a new question.")
        return

    is_correct, feedback = check_answer(answer, question['answer'])
    points = await calculate_points(user_id, is_correct, question['difficulty'])
    
    await update_all_scores(user_id, question['id'], is_correct, points)
    
    if is_correct:
        await ctx.send(f"🎉 Correct! You've earned {points} points. {feedback}\n\n"
                       f"📊 Track your progress with these commands:\n"
                       f"• `!daily_progress` - See your progress for today\n"
                       f"• `!weekly_progress` - Check your weekly progress\n"
                       f"• `!my_achievements` - View your achievements\n")
        await user_questions.pop(user_id, None)
        await user_attempts.pop(user_id, None)
    else:
        max_attempts = await get_max_attempts(user_id, question['id'])
        current_attempts = await user_attempts.get(user_id, 0)
        if current_attempts < max_attempts - 1:
            await user_attempts.set(user_id, current_attempts + 1)
            await ctx.send(
    f"❌ Incorrect. {points} points deducted. {feedback}\n"
    f"You have {max_attempts - current_attempts - 1} attempts left. "
    "Use `!try_again` to attempt this question again.\n\n"
    "If you believe the question is incorrect, use "
    "`!report <question_id> <your feedback>` so we can review it."
)
        else:
            await ctx.send(f"❌ Incorrect. {points} points deducted. You've used all your attempts for this question. The correct answer was:\n```sql\n{question['answer']}\n```")
            await user_questions.pop(user_id, None)
            await user_attempts.pop(user_id, None)

@bot.command()
@db_connection_required()
async def my_stats(ctx):
    user_id = ctx.author.id
    stats = await get_user_stats(user_id)
    
    if stats and stats['total_answers'] > 0:
        success_rate = (stats['correct_answers'] / stats['total_answers']) * 100
        await ctx.send(f"📊 Your SQL Journey Stats 📊\n\n"
                       f"🔢 Total Questions: {stats['total_answers']}\n"
                       f"✅ Correct Answers: {stats['correct_answers']}\n"
                       f"📈 Success Rate: {success_rate:.2f}%\n"
                       f"💰 Total Points: {stats['total_points']}\n\n"
                       f"🌟 Keep coding and climbing the ranks! 🚀\n"
                       f"Remember, every query makes you stronger! 💪")
    else:
        await ctx.send(" Your SQL Adventure Awaits! 🚀\n\n"
                       "You haven't answered any questions yet. Let's change that!\n"
                       "Use `!sql` to get your first question and start your journey.\n\n"
                       "Remember, every SQL master started as a beginner. Your coding adventure begins now! 💪✨")

@tasks.loop(time=time(hour=0, minute=0))  # Midnight IST
async def reset_daily_points():
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                await conn.execute('''
                    DELETE FROM daily_points
                    WHERE date < CURRENT_DATE
                ''')
        logging.info("Daily points reset successfully")
    except Exception as e:
        logging.error(f"Error in reset_daily_points task: {e}")

@bot.event
async def on_ready():
    update_leaderboard.start()
    update_weekly_heroes.start()
    daily_challenge.start()
    challenge_time_over.start()
    logging.info(f'{bot.user} has connected to Discord!')

@bot.event
async def on_error(event, *args, **kwargs):
    logging.error(f"Unhandled error in {event}", exc_info=True)

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandOnCooldown):
        await ctx.send(f"This command is on cooldown. Try again in {error.retry_after:.2f} seconds.")
    elif isinstance(error, commands.CommandNotFound):
        await ctx.send("Unknown command. Use !help to see available commands.")
    else:
        logging.error(f"Unhandled command error: {error}", exc_info=True)
        await ctx.send("An error occurred while processing the command. Please try again later.")

@bot.event
async def on_disconnect():
    print("Bot disconnected from Discord")

@bot.command(name='help')
async def help(ctx):
    help_text = """
    Available commands:
    `!sql`: Get a random SQL question based on your preference
    `!easy`, `!medium`, `!hard`: Get a question of specific difficulty
    `!topic`: List all available topics or get a question on a specific SQL topic
    `!company`: List all available companies or practice questions from a specific company
    `!submit <answer>`: Submit your answer to the current question
    `!my_stats`: Check your personal progress and achievements
    `!set_preference <difficulty>`: Set your preferred question difficulty
    `!reset_preference`: Reset your difficulty preference
    `!submit_question <your question>`: Submit a new question for review

    For more detailed help on each command, use `!help <command_name>`.
    """
    await ctx.send(help_text)

@bot.command()
async def easy(ctx):
    await get_difficulty_question(ctx, 'easy')

@bot.command()
async def medium(ctx):
    await get_difficulty_question(ctx, 'medium')

@bot.command()
async def hard(ctx):
    await get_difficulty_question(ctx, 'hard')

async def get_difficulty_question(ctx, difficulty):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)
    try:
        question = await get_question(difficulty=difficulty, user_id=user_id)
        if question:
            await user_questions.set(user_id, question)
            await user_attempts.set(user_id, 0)
            await display_question(ctx, question)
        else:
            await ctx.send(f"Sorry, no new {difficulty} questions available at the moment.")
    except Exception as e:
        logging.error(f"Error in {difficulty} command: {e}")
        await ctx.send("An error occurred while fetching a question. Please try again later.")

@bot.command()
async def question(ctx):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)

    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                preference = await conn.fetchval('''
                    SELECT preferred_difficulty FROM user_preferences
                    WHERE user_id = $1
                ''', user_id)
        
        question = await get_question(difficulty=preference, user_id=user_id)
        if question:
            await user_questions.set(user_id, question)
            await user_attempts.set(user_id, 0)
            await display_question(ctx, question)
        else:
            await ctx.send("Sorry, no new questions available at your preferred difficulty. Try `!reset_preference` to see questions from all difficulties, or use `!topic <topic>` to try a specific topic.")
    except Exception as e:
        logging.error(f"Error in question command: {e}")
        await ctx.send("An error occurred while fetching a question. Please try again later.")

@bot.command()
async def try_again(ctx):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    question = await user_questions.get(user_id)
    if not question:
        await ctx.send("You don't have an active question. Use `!sql` to get a new question.")
        return

    max_attempts = await get_max_attempts(user_id, question['id'])
    current_attempts = await user_attempts.get(user_id, 0)
    if current_attempts < max_attempts:
        await display_question(ctx, question)
    else:
        await ctx.send("You've used all your attempts for this question. Use `!sql` to get a new question.")

@bot.command()
async def report(ctx, question_id: int, *, feedback):
    user_id = ctx.author.id
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                # Check if the question exists
                question = await conn.fetchrow('SELECT * FROM questions WHERE id = $1', question_id)
                if not question:
                    await ctx.send(f"Question with ID {question_id} does not exist.")
                    return

                # Insert the report
                await conn.execute('''
                    INSERT INTO reports (reported_by, question_id, remarks)
                    VALUES ($1, $2, $3)
                ''', user_id, question_id, feedback)

        await ctx.send(f"Thank you for your feedback. Your report for question {question_id} has been submitted and will be reviewed by our team.")
    except Exception as e:
        logging.error(f"Error in report command: {e}")
        await ctx.send("An error occurred while submitting your report. Please try again later.")

@bot.command()
async def topic(ctx, *, topic_name=None):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)

    if topic_name is None:
        await list_topics(ctx)
        return

    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                topics = await conn.fetch("SELECT DISTINCT topic FROM questions WHERE topic IS NOT NULL")
                topics = [t['topic'].lower() for t in topics]

                best_match = max(topics, key=lambda x: similar(x, topic_name.lower()))
                if similar(best_match, topic_name.lower()) < 0.9:  # 90% accuracy
                    await ctx.send(f"No close match found for '{topic_name}'. Here are the available topics:")
                    await list_topics(ctx)
                    return

                question = await get_question(topic=best_match, user_id=user_id)
                if question:
                    await user_questions.set(user_id, question)
                    await user_attempts.set(user_id, 0)
                    await display_question(ctx, question)
                else:
                    await ctx.send(f"Sorry, no more questions available for the topic '{best_match.title()}' at the moment.")
    except Exception as e:
        logging.error(f"Error in topic question command: {e}")
        await ctx.send("An error occurred while fetching a question. Please try again later.")

async def list_topics(ctx):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                topics = await conn.fetch("SELECT DISTINCT topic FROM questions WHERE topic IS NOT NULL ORDER BY topic")
        
        if topics:
            topic_list = ", ".join([f"{t['topic']}" for t in topics])
            await ctx.send(f"Available topics:\n{topic_list}\n\nUse `!topic <topic name>` to get a question from a specific topic.")
        else:
            await ctx.send("No topics available at the moment.")
    except Exception as e:
        logging.error(f"Error in list_topics: {e}")
        await ctx.send("An error occurred while fetching the topic list. Please try again later.")

@tasks.loop(time=time(hour=22, minute=0))  # 10:00 PM IST
async def update_leaderboard():
    try:
        top_10 = await get_top_10()

        if top_10:
            top_10_message = "🌟 Daily All-Time Top 10 🌟\n\n"
            for i, user in enumerate(top_10, 1):
                emoji = ["🥇", "🥈", "🥉"] + ["🏅"]*7
                top_10_message += f"{emoji[i-1]} {user['username']}: {user['total_points']} points\n"

            for channel_id in CHANNEL_IDS:
                channel = bot.get_channel(channel_id)
                if channel:
                    await channel.send(top_10_message)

    except Exception as e:
        logging.error(f"Error in update_leaderboard task: {e}")

@update_leaderboard.error
async def update_leaderboard_error(error):
    logging.error(f"Unhandled error in update_leaderboard task: {error}", exc_info=True)

@tasks.loop(time=time(hour=17, minute=30))  # 5:30 PM IST
async def daily_challenge():
    try:
        # Get a fresh question that hasn't been used in a challenge before
        question = await get_fresh_challenge_question()
        if question:
            challenge_points = {'easy': 120, 'medium': 160, 'hard': 240}.get(question['difficulty'], 150)
            challenge_message = (
                "🌟 Daily SQL Challenge 🌟\n\n"
                f"Here's today's challenge (worth {challenge_points} points):\n\n"
                f"Question ID: {question['id']}\n"
                f"Difficulty: {question['difficulty'].capitalize()}\n"
                f"{question['question']}\n\n"
                f"Dataset:\n```\n{question['datasets']}\n```\n\n"
                "⏳ You have 4 hours to submit your answer!\n"
                "Use `!submit_challenge` followed by your SQL query to answer."
            )
            for channel_id in CHANNEL_IDS:
                channel = bot.get_channel(channel_id)
                if channel:
                    await channel.send(challenge_message)
            
            # Store the current challenge question
            await set_current_challenge(question['id'])
        else:
            logging.warning("No fresh questions available for the daily challenge.")
    except Exception as e:
        logging.error(f"Error in daily_challenge task: {e}")

async def get_fresh_challenge_question():
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                question = await conn.fetchrow('''
                    SELECT q.* FROM questions q
                    LEFT JOIN challenge_history ch ON q.id = ch.question_id
                    WHERE ch.question_id IS NULL
                    ORDER BY RANDOM()
                    LIMIT 1
                ''')
                if question:
                    await conn.execute('''
                        INSERT INTO challenge_history (question_id, challenge_date)
                        VALUES ($1, $2)
                    ''', question['id'], get_ist_time().date())
        return question
    except Exception as e:
        logging.error(f"Error getting fresh challenge question: {e}")
        return None

async def set_current_challenge(question_id):
    await bot.db.execute('''
        INSERT INTO current_challenge (question_id, start_time)
        VALUES ($1, $2)
        ON CONFLICT (id) DO UPDATE SET question_id = $1, start_time = $2
    ''', question_id, get_ist_time())

@bot.command()
async def submit_challenge(ctx, *, answer):
    user_id = ctx.author.id
    current_challenge = await get_current_challenge()
    if not current_challenge:
        await ctx.send("There is no active challenge right now.")
        return

    await ctx.send("Thank you for submitting your answer. Final results will be shared when the challenge ends.")
    
    is_correct, _ = check_answer(answer, current_challenge['answer'])
    points = {'easy': 120, 'medium': 160, 'hard': 240}.get(current_challenge['difficulty'], 150)
    if not is_correct:
        points = -10  # Deduct points for incorrect answers

    await update_user_stats(user_id, current_challenge['id'], is_correct, points)

@tasks.loop(time=time(hour=21, minute=30))  # 9:30 PM IST
async def challenge_time_over():
    try:
        current_challenge = await get_current_challenge()
        if not current_challenge:
            return

        correct_submissions = await get_correct_challenge_submissions(current_challenge['id'])
        incorrect_submissions = await get_incorrect_challenge_submissions(current_challenge['id'])

        challenge_over_message = (
            "🕒 Daily SQL Challenge Time Over 🕒\n\n"
            f"The challenge for today (Question ID: {current_challenge['id']}) has ended.\n\n"
            "Correct Submissions:\n"
        )
        for submission in correct_submissions:
            challenge_over_message += f"- {submission['username']}\n"
        
        challenge_over_message += "\nIncorrect Submissions:\n"
        for submission in incorrect_submissions:
            challenge_over_message += f"- {submission['username']}\n"
        
        challenge_over_message += "\nIf you missed today's challenge or submitted an incorrect answer, don't worry! A new challenge will be posted tomorrow at 5:30 PM IST. Keep practicing and improving your SQL skills! 💪"

        for channel_id in CHANNEL_IDS:
            channel = bot.get_channel(channel_id)
            if channel:
                await channel.send(challenge_over_message)

        # Clear the current challenge
        await clear_current_challenge()
    except Exception as e:
        logging.error(f"Error in challenge_time_over task: {e}")

async def get_current_challenge():
    return await bot.db.fetchrow('SELECT * FROM current_challenge')

async def clear_current_challenge():
    await bot.db.execute('DELETE FROM current_challenge')

async def get_correct_challenge_submissions(question_id):
    return await bot.db.fetch('''
        SELECT u.username FROM user_submissions us
        JOIN users u ON us.user_id = u.user_id
        WHERE us.question_id = $1 AND us.is_correct = TRUE
    ''', question_id)

async def get_incorrect_challenge_submissions(question_id):
    return await bot.db.fetch('''
        SELECT u.username FROM user_submissions us
        JOIN users u ON us.user_id = u.user_id
        WHERE us.question_id = $1 AND us.is_correct = FALSE
    ''', question_id)

def setup_logging():
    logger = logging.getLogger('discord')
    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler(
        filename='discord.log',
        encoding='utf-8',
        maxBytes=32 * 1024 * 1024,  # 32 MiB
        backupCount=5,
    )
    dt_fmt = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter('[{asctime}] [{levelname:<8}] {name}: {message}', dt_fmt, style='{')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

logger = setup_logging()

# Then use logger.info(), logger.error(), etc. instead of print() throughout your code

async def question_timer(ctx, question_id, time_limit):
    await asyncio.sleep(time_limit * 60)  # Convert minutes to seconds
    user_id = ctx.author.id
    current_question = await user_questions.get(user_id)
    if current_question and current_question['id'] == question_id:
        await ctx.send(f"⏰ Time's up! The question (ID: {question_id}) has expired. Use `!sql` to get a new question.")
        await user_questions.pop(user_id, None)
        await user_attempts.pop(user_id, None)

async def get_challenge_questions(num_questions=5):
    async with DB_SEMAPHORE:
        async with bot.db.acquire() as conn:
            questions = await conn.fetch('''
                SELECT * FROM questions
                ORDER BY RANDOM()
                LIMIT $1
            ''', num_questions)
    return questions

@bot.command()
async def challenge(ctx, num_questions: int = 5):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)

    questions = await get_challenge_questions(num_questions)
    if not questions:
        await ctx.send("Unable to start a challenge. Not enough questions available.")
        return

    await ctx.send(f"Challenge started! You will answer {num_questions} questions with individual time limits.")
    
    start_time = datetime.now()
    correct_answers = 0

    for i, question in enumerate(questions, 1):
        await ctx.send(f"Question {i}/{num_questions}:")
        time_limit = await display_question(ctx, question)

        def check(m):
            return m.author == ctx.author and m.content.startswith('!submit')

        try:
            msg = await bot.wait_for('message', check=check, timeout=time_limit * 60)  # Convert minutes to seconds
            answer = msg.content[8:].strip()  # Remove '!submit ' from the beginning
            if similar(answer.lower(), question['answer'].lower()) >= 0.8:
                await ctx.send("Correct!")
                correct_answers += 1
            else:
                await ctx.send(f"Incorrect. The correct answer was: {question['answer']}")
        except asyncio.TimeoutError:
            await ctx.send(f"Time's up for this question! Moving to the next one.")

    total_time = (datetime.now() - start_time).total_seconds() / 60
    await ctx.send(f"Challenge complete! You answered {correct_answers}/{num_questions} questions correctly in {total_time:.2f} minutes.")

    # Update user stats
    async with DB_SEMAPHORE:
        async with bot.db.acquire() as conn:
            await conn.execute('''
                INSERT INTO user_challenges (user_id, total_questions, correct_answers, time_taken)
                VALUES ($1, $2, $3, $4)
            ''', user_id, num_questions, correct_answers, total_time)

@bot.command()
async def challenge_history(ctx):
    user_id = ctx.author.id
    async with DB_SEMAPHORE:
        async with bot.db.acquire() as conn:
            history = await conn.fetch('''
                SELECT * FROM user_challenges
                WHERE user_id = $1
                ORDER BY completed_at DESC
                LIMIT 5
            ''', user_id)

    if history:
        await ctx.send("Your recent challenge history:")
        for challenge in history:
            await ctx.send(f"Date: {challenge['completed_at'].replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=5, minutes=30)))}, Score: {challenge['correct_answers']}/{challenge['total_questions']}, Time: {challenge['time_taken']:.2f} minutes")
    else:
        await ctx.send("You haven't completed any challenges yet.")

class SQLBattle:
    def __init__(self, channel, players, num_questions=5):
        self.channel = channel
        self.players = players
        self.num_questions = num_questions
        self.scores = {player.id: 0 for player in players}
        self.current_question = None
        self.answered = False

    async def start(self):
        await self.channel.send("SQL Battle started! Get ready for the questions!")
        for i in range(self.num_questions):
            await self.ask_question()
        await self.end_battle()

    async def ask_question(self):
        self.current_question = await get_question()
        self.answered = False
        time_limit = await display_question(self.channel, self.current_question)
        
        try:
            def check(m):
                return m.author in self.players and m.content.startswith('!submit')
            
            msg = await bot.wait_for('message', check=check, timeout=time_limit * 60)
            await self.process_answer(msg)
        except asyncio.TimeoutError:
            await self.channel.send("Time's up! No one answered correctly.")

    async def process_answer(self, msg):
        if self.answered:
            return
        
        answer = msg.content[8:].strip()
        if similar(answer.lower(), self.current_question['answer'].lower()) >= 0.8:
            self.scores[msg.author.id] += 1
            self.answered = True
            await self.channel.send(f"{msg.author.mention} answered correctly! They get a point!")
        else:
            await self.channel.send(f"{msg.author.mention}'s answer is incorrect.")

    async def end_battle(self):
        sorted_scores = sorted(self.scores.items(), key=lambda x: x[1], reverse=True)
        result = "Final Scores:\n"
        for player_id, score in sorted_scores:
            player = bot.get_user(player_id)
            result += f"{player.name}: {score} points\n"
        await self.channel.send(result)
        winner = bot.get_user(sorted_scores[0][0])
        await self.channel.send(f"🎉 {winner.mention} wins the SQL Battle! 🏆")

@bot.command()
@commands.cooldown(1, 5, commands.BucketType.user)
async def sql_battle(ctx):
    await user_last_active.set(ctx.author.id, datetime.now(timezone.utc))
    await ctx.send("SQL Battle is starting! React with 👍 to join. The battle will begin in 30 seconds.")
    message = await ctx.send("Waiting for players...")
    await message.add_reaction("👍")

    await asyncio.sleep(30)

    message = await ctx.channel.fetch_message(message.id)
    reaction = discord.utils.get(message.reactions, emoji="👍")
    players = []
    async for user in reaction.users():
        if not user.bot:
            players.append(user)

    if len(players) < 2:
        await ctx.send("Not enough players to start the battle. At least 2 players are required.")
        return

    battle = SQLBattle(ctx.channel, players)
    await battle.start()

@bot.command()
async def set_difficulty(ctx, difficulty: str):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    valid_difficulties = ['easy', 'medium', 'hard']
    difficulty_emojis = {'easy': '🟢', 'medium': '🟡', 'hard': '🔴'}
    
    if difficulty.lower() not in valid_difficulties:
        await ctx.send(f"❌ Invalid difficulty. Please choose from: {', '.join(valid_difficulties)}")
        return

    async with DB_SEMAPHORE:
        async with bot.db.acquire() as conn:
            await conn.execute('''
                INSERT INTO user_preferences (user_id, preferred_difficulty)
                VALUES ($1, $2)
                ON CONFLICT (user_id) DO UPDATE SET preferred_difficulty = $2
            ''', user_id, difficulty.lower())
    
    emoji = difficulty_emojis[difficulty.lower()]
    await ctx.send(f"{emoji} Great choice! Your preferred difficulty has been set to **{difficulty}**.\n\n"
                   f"📊 To check your current difficulty, use `!my_difficulty`\n"
                   f"🔄 To reset your difficulty preference, use `!reset_preference`")

@bot.command()
async def reset_preference(ctx):
    user_id = ctx.author.id
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                await conn.execute('''
                    UPDATE user_preferences
                    SET preferred_difficulty = NULL
                    WHERE user_id = $1
                ''', user_id)
        await ctx.send("🔄 Your difficulty preference has been reset. You'll now receive questions from all difficulties.\n\n"
                       "To set a new preference, use `!set_difficulty <easy/medium/hard>`")
    except Exception as e:
        logging.error(f"Error in reset_preference command: {e}")
        await ctx.send("An error occurred while resetting your preference. Please try again later.")

@bot.command()
async def rate_question(ctx, question_id: int, rating: int):
    if rating < 1 or rating > 5:
        await ctx.send("Please rate the question from 1 to 5.")
        return

    user_id = ctx.author.id
    async with DB_SEMAPHORE:
        async with bot.db.acquire() as conn:
            await conn.execute('''
                INSERT INTO question_ratings (user_id, question_id, rating)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id, question_id) DO UPDATE SET rating = $3
            ''', user_id, question_id, rating)
    
    await ctx.send(f"Thank you for rating question {question_id}!")

@bot.command()
async def question_stats(ctx, question_id: int):
    async with DB_SEMAPHORE:
        async with bot.db.acquire() as conn:
            stats = await conn.fetchrow('''
                SELECT AVG(rating) as avg_rating, COUNT(*) as total_ratings
                FROM question_ratings
                WHERE question_id = $1
            ''', question_id)
    
    if stats['total_ratings'] > 0:
        await ctx.send(f"Question {question_id} stats:\nAverage rating: {stats['avg_rating']:.2f}\nTotal ratings: {stats['total_ratings']}")
    else:
        await ctx.send(f"No ratings yet for question {question_id}.")

async def check_achievements(user_id):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                stats = await get_user_stats(user_id)
                
                new_achievements = []
                all_achievements = []
                
                achievement_criteria = [
                    ("🎓 Beginner", stats['total_answers'] >= 10),
                    ("🏅 Intermediate", stats['total_answers'] >= 100),
                    ("🏆 Expert", stats['total_answers'] >= 1000),
                    (" Sharpshooter", stats['correct_answers'] >= 50),
                    ("👑 SQL Master", stats['correct_answers'] >= 500)
                ]
                
                for achievement, condition in achievement_criteria:
                    if condition:
                        all_achievements.append(achievement)
                        result = await conn.fetchval('''
                            INSERT INTO user_achievements (user_id, achievement)
                            VALUES ($1, $2)
                            ON CONFLICT (user_id, achievement) DO NOTHING
                            RETURNING achievement
                        ''', user_id, achievement)
                        if result:
                            new_achievements.append(achievement)
                
                return new_achievements, all_achievements
    except Exception as e:
        logging.error(f"Error in check_achievements: {e}")
        return [], []  # Return empty lists if there's an error

@bot.command()
async def my_achievements(ctx):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))  # Add this line
    async with DB_SEMAPHORE:
        async with bot.db.acquire() as conn:
            achievements = await conn.fetch('''
                SELECT achievement FROM user_achievements
                WHERE user_id = $1
            ''', user_id)
    
    if achievements:
        achievement_list = "\n".join([f" {a['achievement']}" for a in achievements])
        await ctx.send(f"🌟 Your SQL Trophy Case 🌟\n\n{achievement_list}\n\n"
                       f"Impressive collection, SQL champion! 🎉\n"
                       f"What will you conquer next? 🚀")
    else:
        await ctx.send("🌱 Your Achievement Journey Begins! 🌱\n\n"
                       "You haven't earned any achievements yet, but fear not!\n"
                       "Every query brings you closer to SQL greatness.\n"
                       "Keep practicing, and soon you'll be swimming in achievements! 🏊‍♂️🏆")

# Call this function after each question submission
async def update_user_achievements(ctx, user_id):
    try:
        new_achievements, _ = await check_achievements(user_id)
        if new_achievements:
            user = await bot.fetch_user(user_id)
            achievement_message = f"🎉 Congratulations to {user.name}! They've earned new achievements: {', '.join(new_achievements)}"
            
            # Send to the user
            await ctx.send(f"Congratulations! You've earned new achievements: {', '.join(new_achievements)}")
            
            # Send to the specified bot channels
            for channel_id in CHANNEL_IDS:
                channel = bot.get_channel(channel_id)
                if channel:
                    await channel.send(achievement_message)
                else:
                    logging.warning(f"Channel with ID {channel_id} not found")
    except Exception as e:
        logging.error(f"Error in update_user_achievements: {e}")
        # Don't send an error message to the user for this internal error

async def ensure_tables_exist():
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        username VARCHAR(255) NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS scheduled_posts (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                        message TEXT NOT NULL,
                        posted BOOLEAN DEFAULT FALSE
                    );
                                   
                    CREATE TABLE IF NOT EXISTS questions (
                        id SERIAL PRIMARY KEY,
                        question TEXT NOT NULL,
                        answer TEXT NOT NULL,
                        difficulty VARCHAR(10) NOT NULL,
                        topic VARCHAR(255),
                        company VARCHAR(255)
                    );

                    CREATE TABLE IF NOT EXISTS user_submissions (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        question_id INT,
                        is_correct BOOLEAN,
                        points INT,
                        submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS user_preferences (
                        user_id BIGINT PRIMARY KEY,
                        preferred_difficulty VARCHAR(10)
                    );

                    CREATE TABLE IF NOT EXISTS weekly_points (
                        user_id BIGINT,
                        points INT,
                        week_start DATE,
                        PRIMARY KEY (user_id, week_start)
                    );

                    CREATE TABLE IF NOT EXISTS daily_points (
                        user_id BIGINT,
                        date DATE,
                        points INT,
                        PRIMARY KEY (user_id, date)
                    );

                    CREATE TABLE IF NOT EXISTS leaderboard (
                        user_id BIGINT PRIMARY KEY,
                        points INT
                    );

                    CREATE TABLE IF NOT EXISTS user_achievements (
                        user_id BIGINT,
                        achievement VARCHAR(255),
                        PRIMARY KEY (user_id, achievement)
                    );

                    CREATE TABLE IF NOT EXISTS submitted_questions (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        username VARCHAR(255),
                        question TEXT,
                        submitted_at TIMESTAMP
                    );
                                   
                    CREATE TABLE IF NOT EXISTS reports (
                        id SERIAL PRIMARY KEY,
                        reported_by BIGINT,
                        question_id INT,
                        remarks TEXT,
                        reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS challenge_history (
                        question_id INTEGER PRIMARY KEY,
                        challenge_date DATE NOT NULL
                    );
                ''')
        logging.info("All tables created successfully")
    except Exception as e:
        logging.error(f"Error ensuring tables exist: {e}")
        raise

async def wait_for_db():
    max_retries = 5
    retry_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            await create_db_pool()
            await ensure_tables_exist()
            logging.info("Database connection established and tables verified.")
            return
        except Exception as e:
            logging.error(f"Attempt {attempt + 1}/{max_retries} to connect to the database failed: {e}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                logging.error("Max retries reached. Unable to connect to the database.")
                raise

async def graceful_shutdown():
    print("Shutting down gracefully...")
    if hasattr(bot, 'db'):
        await bot.db.close()
    await bot.close()

@bot.command()
async def check_db(ctx):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    try:
        result = await db_operation(lambda conn: conn.fetchval("SELECT 1"))
        if result == 1:
            await ctx.send("Database connection is working!")
        else:
            await ctx.send("Database connection test failed.")
    except Exception as e:
        logging.error(f"Error checking database connection: {e}")
        await ctx.send("An error occurred while checking the database connection.")

@tasks.loop(time=time(hour=0, minute=0))  # Midnight IST
async def daily_task():
    try:
        # Add any daily maintenance or update tasks here
        logging.info("Daily task completed successfully")
    except Exception as e:
        logging.error(f"Error in daily task: {e}", exc_info=True)

@daily_task.error
async def daily_task_error(error):
    logging.error(f"Unhandled error in daily task: {error}", exc_info=True)

async def calculate_points(user_id, is_correct, difficulty):
    base_points = {'easy': 120, 'medium': 160, 'hard': 240}.get(difficulty, 0)  # Doubled
    if is_correct:
        streak = await get_user_streak(user_id)
        streak_bonus = min(streak * 10, 100)  # Doubled streak bonus, capped at 100
        return base_points + streak_bonus
    else:
        return -10  # Doubled deduction for incorrect answers

async def get_topic_question(ctx, topic_name):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                topics = await conn.fetch("SELECT DISTINCT topic FROM questions WHERE topic IS NOT NULL")
                topics = [t['topic'].lower() for t in topics]

        best_match = max(topics, key=lambda x: similar(x, topic_name.lower()))
        if similar(best_match, topic_name.lower()) < 0.9:  # 90% accuracy
            await ctx.send(f"No close match found for '{topic_name}'. Here are the available topics:")
            await list_topics(ctx)
            return

        question = await get_question(topic=best_match, user_id=user_id)
        if question:
            await user_questions.set(user_id, question)
            await display_question(ctx, question)
        else:
            await ctx.send(f"Sorry, no questions available for the topic '{best_match.title()}' at the moment.")
    except Exception as e:
        logging.error(f"Error in topic question command: {e}")
        await ctx.send("An error occurred while fetching a question. Please try again later.")

def get_ist_time():
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))

@bot.command()
async def set_preference(ctx, *, preference=None):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)

    try:
        difficulties = ['easy', 'medium', 'hard']

        if not preference:
            difficulty_options = ", ".join(difficulties)
            await ctx.send(f"Available difficulties: {difficulty_options}\n"
                           f"To set your preference, use `!set_preference <difficulty>`")
            return

        if preference.lower() not in difficulties:
            difficulty_options = ", ".join(difficulties)
            await ctx.send(f"Invalid preference. Available options are: {difficulty_options}\n"
                           f"Please try again with a valid option.")
            return

        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                await conn.execute('''
                    INSERT INTO user_preferences (user_id, preferred_difficulty)
                    VALUES ($1, $2)
                    ON CONFLICT (user_id) DO UPDATE SET preferred_difficulty = $2
                ''', user_id, preference.lower())

        await ctx.send(f"Your preferred difficulty has been set to '{preference}'. "
                       f"You can reset it anytime using the `!reset_preference` command.")

    except Exception as e:
        logging.error(f"Error in set_preference command: {e}")
        await ctx.send("An error occurred while setting your preference. Please try again later.")

@bot.command()
async def submit_question(ctx, *, question):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    username = str(ctx.author)
    submitted_at = datetime.now(timezone.utc)
    
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                await conn.execute('''
                    INSERT INTO submitted_questions (user_id, username, question, submitted_at)
                    VALUES ($1, $2, $3, $4)
                ''', user_id, username, question, submitted_at)
        
        await ctx.send("Thank you for your contribution! Your question has been submitted for review. All contributor names will be added to GitHub monthly.")
    except Exception as e:
        logging.error(f"Error in submit_question command: {e}")
        await ctx.send("An error occurred while submitting your question. Please try again later.")

@bot.command()
async def daily_progress(ctx):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    today = get_ist_time().date()
    
    async with DB_SEMAPHORE:
        async with bot.db.acquire() as conn:
            daily_points = await conn.fetchval('''
                SELECT COALESCE(SUM(points), 0)
                FROM user_submissions
                WHERE user_id = $1 AND DATE(submitted_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') = $2
            ''', user_id, today)
            daily_submissions = await conn.fetchval('''
                SELECT COUNT(*)
                FROM user_submissions
                WHERE user_id = $1 AND DATE(submitted_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') = $2
            ''', user_id, today)
    
    await ctx.send(f"📊 Your Daily Progress 📊\n"
                   f"Points earned today: {daily_points}\n"
                   f"Questions attempted: {daily_submissions}\n"
                   f"Keep up the great work! 💪")

@bot.command()
async def weekly_progress(ctx):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    week_start = await get_week_start()
    
    async with DB_SEMAPHORE:
        async with bot.db.acquire() as conn:
            weekly_points = await conn.fetchval('''
                SELECT COALESCE(SUM(points), 0)
                FROM user_submissions
                WHERE user_id = $1 AND submitted_at >= $2
            ''', user_id, week_start)
    
    await ctx.send(f"🗓️ Your Weekly Progress 🗓️\n"
                   f"Points earned this week: {weekly_points}\n"
                   f"You're making great strides! 🚀")

@bot.command()
async def company(ctx, *, company_name=None):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)

    if company_name is None:
        await list_companies(ctx)
        return

    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                # Get all companies
                companies = await conn.fetch("SELECT DISTINCT company FROM questions WHERE company IS NOT NULL")
                companies = [c['company'].lower() for c in companies]

                # Find the best match
                best_match = max(companies, key=lambda x: similar(x, company_name.lower()))
                if similar(best_match, company_name.lower()) < 0.7:
                    await ctx.send(f"No close match found for '{company_name}'. Here are the available companies:")
                    await list_companies(ctx)
                    return

                question = await get_question(company=best_match, user_id=user_id)
                if question:
                    await user_questions.set(user_id, question)
                    await user_attempts.set(user_id, 0)
                    await display_question(ctx, question)
                else:
                    await ctx.send(f"Sorry, no new questions available for the company '{best_match.title()}' at the moment.")
    except Exception as e:
        logging.error(f"Error in company question command: {e}")
        await ctx.send("An error occurred while fetching a question. Please try again later.")

async def list_companies(ctx):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                companies = await conn.fetch("SELECT DISTINCT company FROM questions WHERE company IS NOT NULL ORDER BY company")
        
        if companies:
            company_list = ", ".join([f"{c['company']}" for c in companies])
            await ctx.send(f"Available companies:\n{company_list}\n\nUse `!company <company name>` to get a question from a specific company.")
        else:
            await ctx.send("No companies available at the moment.")
    except Exception as e:
        logging.error(f"Error in list_companies: {e}")
        await ctx.send("An error occurred while fetching the company list. Please try again later.")

def check_answer(user_answer, correct_answer):
    # Normalize and parse the SQL queries
    user_sql = sqlparse.format(user_answer.strip().lower(), reindent=True, keyword_case='upper')
    correct_sql = sqlparse.format(correct_answer.strip().lower(), reindent=True, keyword_case='upper')

    # Compare the parsed SQL structures
    user_parsed = sqlparse.parse(user_sql)
    correct_parsed = sqlparse.parse(correct_sql)

    # Calculate similarity
    structure_similarity = compare_sql_structures(user_parsed, correct_parsed)
    string_similarity = SequenceMatcher(None, user_sql, correct_sql).ratio()
    overall_similarity = (structure_similarity + string_similarity) / 2

    is_correct = overall_similarity >= 0.65
    feedback = f"Similarity: {overall_similarity:.2%}"
    return is_correct, feedback

def compare_sql_structures(user_parsed, correct_parsed):
    def get_tokens(parsed):
        return [token for stmt in parsed for token in stmt.flatten() if not token.is_whitespace]

    user_tokens = get_tokens(user_parsed)
    correct_tokens = get_tokens(correct_parsed)

    common_tokens = set(token.normalized for token in user_tokens) & set(token.normalized for token in correct_tokens)
    return len(common_tokens) / max(len(user_tokens), len(correct_tokens))

async def get_user_streak(user_id):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                streak = await conn.fetchval('''
                    SELECT streak FROM user_stats WHERE user_id = $1
                ''', user_id)
        return streak or 0
    except Exception as e:
        logging.error(f"Error getting user streak: {e}")
        return 0

async def update_user_streak(user_id, is_correct):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                if is_correct:
                    await conn.execute('''
                        INSERT INTO user_stats (user_id, streak)
                        VALUES ($1, 1)
                        ON CONFLICT (user_id)
                        DO UPDATE SET streak = user_stats.streak + 1
                    ''', user_id)
                else:
                    await conn.execute('''
                        UPDATE user_stats SET streak = 0 WHERE user_id = $1
                    ''', user_id)
    except Exception as e:
        logging.error(f"Error updating user streak: {e}")

user_locks = {}

async def get_user_lock(user_id):
    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Lock()
    return user_locks[user_id]

# Use the lock in critical sections, e.g.:
async def update_user_data(user_id, data):
    async with await get_user_lock(user_id):
        # Update user data here
        pass  # Replace this with actual update logic

async def setup():
    required_vars = ['DATABASE_URL', 'DISCORD_TOKEN', 'CHANNEL_ID']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        for var in required_vars:
            print(f"{var}: {os.getenv(var)}")
        raise ValueError("Missing required environment variables")

    await wait_for_db()

async def db_operation(operation, *args):
    async with DB_SEMAPHORE:
        try:
            async with bot.db.acquire() as conn:
                return await operation(conn, *args)
        except asyncpg.InterfaceError as e:
            logging.error(f"Database interface error: {e}")
            raise
        except Exception as e:
            logging.error(f"Database operation error: {e}")
            raise
# -- updated code
async def update_user_stats(user_id, question_id, is_correct, points):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                await conn.execute('''
                    INSERT INTO user_submissions (user_id, question_id, is_correct, points)
                    VALUES ($1, $2, $3, $4)
                ''', user_id, question_id, is_correct, points)
        
        # Update weekly points
        await update_weekly_points(user_id, points)
        
        # Update daily points
        today = get_ist_time().date()
        await update_daily_points(user_id, today, points)
        
    except Exception as e:
        logging.error(f"Error updating user stats: {e}")
        raise

async def get_max_attempts(user_id, question_id):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                incorrect_submissions = await conn.fetchval('''
                    SELECT COUNT(*) FROM user_submissions
                    WHERE user_id = $1 AND question_id = $2 AND is_correct = FALSE
                ''', user_id, question_id)
        return max(5 - incorrect_submissions, 1)  # Minimum 1 attempt, maximum 5
    except Exception as e:
        logging.error(f"Error getting max attempts: {e}")
        return 5  # Default to 5 if there's an error

async def get_weekly_heroes():
    try:
        week_start = await get_week_start()
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                heroes = await conn.fetch('''
                    SELECT u.username, SUM(us.points) as total_points
                    FROM user_submissions us
                    JOIN users u ON us.user_id = u.user_id
                    WHERE us.submitted_at >= $1
                    GROUP BY u.user_id, u.username
                    ORDER BY total_points DESC
                    LIMIT 5
                ''', week_start)
        return heroes
    except Exception as e:
        logging.error(f"Error getting weekly heroes: {e}")
        return []

async def get_top_10():
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                top_users = await conn.fetch('''
                    SELECT u.username, SUM(us.points) as total_points
                    FROM user_submissions us
                    JOIN users u ON us.user_id = u.user_id
                    GROUP BY u.user_id, u.username
                    ORDER BY total_points DESC
                    LIMIT 10
                ''')
        return top_users
    except Exception as e:
        logging.error(f"Error getting top 10: {e}")
        return []

@bot.command()
async def update_leaderboards(ctx):
    if ctx.author.id not in ADMIN_IDS:
        await ctx.send("You don't have permission to use this command.")
        return
    await update_leaderboard()
    await ctx.send("Leaderboards have been manually updated.")

@bot.command()
async def post_monthly_leaderboard(ctx):
    if ctx.author.id not in ADMIN_IDS:
        await ctx.send("You don't have permission to use this command.")
        return
    await post_monthly_leaderboard_function()
    await ctx.send("Monthly leaderboard has been posted.")

@bot.command()
async def view_reports(ctx, limit: int = 10):
    if ctx.author.id not in ADMIN_IDS:
        await ctx.send("You don't have permission to use this command.")
        return
    reports = await get_recent_reports(limit)
    if reports:
        report_text = "Recent Reports:\n\n"
        for report in reports:
            report_text += f"Question ID: {report['question_id']}\n"
            report_text += f"Reported by: {report['reported_by']}\n"
            report_text += f"Remarks: {report['remarks']}\n\n"
        await ctx.send(report_text)
    else:
        await ctx.send("No recent reports found.")

@bot.command()
async def view_stats(ctx):
    if ctx.author.id not in ADMIN_IDS:
        await ctx.send("You don't have permission to use this command.")
        return
    stats = await get_bot_stats()
    stats_text = "Bot Usage Statistics:\n\n"
    stats_text += f"Total Users: {stats['total_users']}\n"
    stats_text += f"Total Questions: {stats['total_questions']}\n"
    stats_text += f"Total Submissions: {stats['total_submissions']}\n"
    await ctx.send(stats_text)

@tasks.loop(time=time(hour=9, minute=0))  # 9:00 AM IST
async def update_weekly_heroes():
    if datetime.now(pytz.timezone('Asia/Kolkata')).weekday() != 6:  # 6 is Sunday
        return  # Only run on Sundays

    try:
        weekly_heroes = await get_weekly_heroes()

        if weekly_heroes:
            heroes_message = "🏆 Weekly Heroes 🏆\n\n"
            for i, hero in enumerate(weekly_heroes, 1):
                emoji = ["🥇", "🥈", "🥉", "", "🏅"][i-1]
                heroes_message += f"{emoji} {hero['username']}: {hero['total_points']} points\n"

            for channel_id in CHANNEL_IDS:
                channel = bot.get_channel(channel_id)
                if channel:
                    await channel.send(heroes_message)

        # Reset weekly points after posting
        await reset_weekly_points()

    except Exception as e:
        logging.error(f"Error in update_weekly_heroes task: {e}")

async def reset_weekly_points():
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                await conn.execute('DELETE FROM weekly_points')
    except Exception as e:
        logging.error(f"Error resetting weekly points: {e}")

@bot.command()
async def admin(ctx):
    if ctx.author.id not in ADMIN_IDS:
        await ctx.send("You don't have permission to use this command.")
        return
    admin_help_text = """
    🛠️ Admin Commands 🛠️

    Here are the available admin commands:

    1. !update_leaderboards
       Usage: !update_leaderboards
       Description: Manually triggers an update of the leaderboards.

    2. !post_monthly_leaderboard
       Usage: !post_monthly_leaderboard
       Description: Manually posts the monthly leaderboard.

    3. !view_reports
       Usage: !view_reports [limit]
       Description: Views recent question reports. Optionally specify a limit (default 10).

    4. !view_stats
       Usage: !view_stats
       Description: Displays overall bot usage statistics.

    Remember, with great power comes great responsibility. Use these commands wisely!
    """
    await ctx.send(admin_help_text)

async def get_recent_reports(limit):
    async with DB_SEMAPHORE:
        async with bot.db.acquire() as conn:
            reports = await conn.fetch('''
                SELECT * FROM reports
                ORDER BY reported_at DESC
                LIMIT $1
            ''', limit)
    return reports

async def get_bot_stats():
    async with DB_SEMAPHORE:
        async with bot.db.acquire() as conn:
            total_users = await conn.fetchval('SELECT COUNT(*) FROM users')
            total_questions = await conn.fetchval('SELECT COUNT(*) FROM questions')
            total_submissions = await conn.fetchval('SELECT COUNT(*) FROM user_submissions')
    return {
        'total_users': total_users,
        'total_questions': total_questions,
        'total_submissions': total_submissions
    }

async def post_monthly_leaderboard_function():
    # Implement the monthly leaderboard posting logic here
    pass

async def update_all_scores(user_id, question_id, is_correct, points):
    try:
        # Update user submissions
        await update_user_stats(user_id, question_id, is_correct, points)
        
        # Update weekly points
        await update_weekly_points(user_id, points)
        
        # Update daily points
        today = get_ist_time().date()
        await update_daily_points(user_id, today, points)
        
        # Update user streak
        await update_user_streak(user_id, is_correct)
        
        # Check and update achievements
        await update_user_achievements(None, user_id)
        
    except Exception as e:
        logging.error(f"Error updating all scores: {e}")
        raise

@bot.command()
async def schedule_post(ctx, *, args=None):
    if ctx.author.id not in ADMIN_IDS:
        await ctx.send("You don't have permission to use this command.")
        return

    if not args:
        usage = (
            "Usage: !schedule_post <timestamp> <message>\n"
            "Timestamp format: 'YYYY-MM-DD HH:MM:SS' (in IST)\n"
            "Example: !schedule_post '2024-10-26 22:00:00' This is a scheduled message"
        )
        await ctx.send(usage)
        return

    try:
        # Split the args into timestamp and message
        timestamp_str, message = args.split(maxsplit=1)
        
        # Parse the timestamp
        timestamp = datetime.strptime(timestamp_str.strip("'"), '%Y-%m-%d %H:%M:%S')
        
        # Convert to IST
        ist = pytz.timezone('Asia/Kolkata')
        timestamp = ist.localize(timestamp)
        
        # Store in the database
        async with bot.db.acquire() as conn:
            await conn.execute('''
                INSERT INTO scheduled_posts (timestamp, message)
                VALUES ($1, $2)
            ''', timestamp, message)
        
        await ctx.send(f"Post scheduled for {timestamp_str} IST")
    except ValueError:
        await ctx.send("Invalid timestamp format. Use 'YYYY-MM-DD HH:MM:SS'")
    except Exception as e:
        logging.error(f"Error scheduling post: {e}")
        await ctx.send("An error occurred while scheduling the post.")

@tasks.loop(time=time(hour=13, minute=30))  # 7:00 PM IST (13:30 UTC)
async def check_scheduled_posts():
    try:
        async with bot.db.acquire() as conn:
            # Get all unposted messages that are due
            now = datetime.now(pytz.utc)
            posts = await conn.fetch('''
                SELECT id, message
                FROM scheduled_posts
                WHERE timestamp <= $1 AND NOT posted
            ''', now)
            
            for post in posts:
                # Post the message to all channels
                for channel_id in CHANNEL_IDS:
                    channel = bot.get_channel(channel_id)
                    if channel:
                        await channel.send(post['message'])
                    else:
                        logging.warning(f"Channel with ID {channel_id} not found")
                
                # Mark as posted
                await conn.execute('''
                    UPDATE scheduled_posts
                    SET posted = TRUE
                    WHERE id = $1
                ''', post['id'])
    except Exception as e:
        logging.error(f"Error in check_scheduled_posts: {e}")

@check_scheduled_posts.before_loop
async def before_check_scheduled_posts():
    await bot.wait_until_ready()

@bot.command()
async def skip(ctx):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    
    current_question = await user_questions.get(user_id)
    if not current_question:
        await ctx.send("You don't have an active question to skip. Use `!sql` to get a new question.")
        return

    # Remove the current question
    await user_questions.pop(user_id, None)
    await user_attempts.pop(user_id, None)

    # Cancel the timer if it exists
    if user_id in user_timers:
        user_timers[user_id].cancel()
        del user_timers[user_id]

    await ctx.send("Question skipped. Use `!sql` to get a new question.")

    # Optionally, you can automatically give a new question here
    # await get_difficulty_question(ctx, current_question['difficulty'])

def main():
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(setup())
        loop.run_until_complete(bot.start(os.getenv('DISCORD_TOKEN')))
    except KeyboardInterrupt:
        print("Bot stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        loop.run_until_complete(graceful_shutdown())
        loop.close()

if __name__ == "__main__":
    main()
