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
from discord.ext.commands import cooldown, BucketType
from logging.handlers import RotatingFileHandler
import pytz
import functools
import sqlparse

# Setup
logging.basicConfig(level=logging.INFO)
load_dotenv()

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

# Add the new get_user_stats function here
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
                    LEFT JOIN reports r ON q.id = r.question_id
                    WHERE (us.question_id IS NULL OR us.points <= 0)
                    AND r.question_id IS NULL
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
    
    message = f"Question ID: {question['id']}\n"
    message += f"Difficulty: {difficulty} ({points} points)\n"
    if question.get('topic'):
        message += f"Topic: {question['topic']}\n"
    if question.get('company'):
        message += f"Company: {question['company']}\n"
    message += f"\n{question['question']}\n"
    if question['datasets']:
        message += f"\nDataset:\n```\n{question['datasets']}\n```"
    message += "\nUse `!submit` followed by your SQL query to answer!"
    
    await ctx.send(message)

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

    await update_user_submissions(user_id, question['id'], is_correct, points)
    await update_weekly_points(user_id, points)
    today = get_ist_time().date()
    await update_daily_points(user_id, today, points)

    if is_correct:
        await ctx.send(f"🎉 Correct! You've earned {points} points. {feedback}")
        await user_questions.pop(user_id, None)
        await user_attempts.pop(user_id, None)
    else:
        current_attempts = await user_attempts.get(user_id, 0)
        max_attempts = 3
        if current_attempts < max_attempts - 1:
            await user_attempts.set(user_id, current_attempts + 1)
            attempts_left = max_attempts - current_attempts - 1
            await ctx.send(f"❌ Sorry, that's not correct. {feedback}\nYou have {attempts_left} {'attempt' if attempts_left == 1 else 'attempts'} left. Use `!try_again` to give it another shot!")
        else:
            await ctx.send(f"❌ Sorry, that's not correct. {feedback}\nUse `!sql` to get a new question.")
            await user_questions.pop(user_id, None)
            await user_attempts.pop(user_id, None)

    await update_user_achievements(ctx, user_id)

# @bot.command()
# @db_connection_required()
# async def my_stats(ctx):
#     user_id = ctx.author.id
#     async with DB_SEMAPHORE:
#         async with bot.db.acquire() as conn:
#             stats = await conn.fetchrow('''
#                 SELECT 
#                     COUNT(*) as total_questions,
#                     SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) as correct_answers,
#                     AVG(points) as avg_points,
#                     SUM(points) as total_points
#                 FROM user_submissions
#                 WHERE user_id = $1
#             ''', user_id)
    
#     if stats and stats['total_questions'] > 0:
#         success_rate = (stats['correct_answers'] / stats['total_questions']) * 100
#         await ctx.send(f"📊 Your SQL Journey Stats 📊\n\n"
#                        f"🔢 Total Questions: {stats['total_questions']}\n"
#                        f"✅ Correct Answers: {stats['correct_answers']}\n"
#                        f"📈 Success Rate: {success_rate:.2f}%\n"
#                        f"⭐ Average Points: {stats['avg_points']:.2f}\n"
#                        f"💰 Total Points: {stats['total_points']}\n\n"
#                        f"🌟 Keep coding and climbing the ranks! 🚀\n"
#                        f"Remember, every query makes you stronger! 💪")
#     else:
#         await ctx.send("🚀 Your SQL Adventure Awaits! 🚀\n\n"
#                        "You haven't answered any questions yet. Let's change that!\n"
#                        "Use `!sql` to get your first question and start your journey.\n\n"
#                        "Remember, every SQL master started as a beginner. Your coding adventure begins now! 💪✨")

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
    print("Bot is ready.")
    if not hasattr(bot, 'db'):
        logging.error("Database connection not established. Shutting down.")
        await bot.close()

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

# @bot.command(name='help')
# async def help(ctx):
#     help_text = """
#     Available commands:
#     `!sql`: Get a random SQL question based on your preference
#     `!easy`, `!medium`, `!hard`: Get a question of specific difficulty
#     `!topic`: List all available topics or get a question on a specific SQL topic
#     `!company`: List all available companies or practice questions from a specific company
#     `!submit <answer>`: Submit your answer to the current question
#     `!report <question_id> <feedback>`: Report an issue with a question
#     `!top_10`: View the current leaderboard
#     `!weekly_heroes`: View this week's top performers
#     `!my_stats`: Check your personal progress and achievements
#     `!set_preference <difficulty>`: Set your preferred question difficulty
#     `!reset_preference`: Reset your difficulty preference
#     `!submit_question <your question>`: Submit a new question for review

#     For more detailed help on each command, use `!help <command_name>`.
#     """
#     await ctx.send(help_text)

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
    await user_last_active.set(user_id, datetime.now(timezone.utc))  # Add this line
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)
    try:
        question = await get_question(difficulty, user_id)
        if question:
            await user_questions.set(user_id, question)
            await display_question(ctx, question)
        else:
            await ctx.send(f"Sorry, no {difficulty} questions available at the moment.")
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
            await display_question(ctx, question)
        else:
            await ctx.send("Sorry, no questions available at the moment.")
    except Exception as e:
        logging.error(f"Error in question command: {e}")
        await ctx.send("An error occurred while fetching a question. Please try again later.")

@bot.command()
async def try_again(ctx):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))
    current_attempts = await user_attempts.get(user_id, 0)
    max_attempts = 3
    if current_attempts < max_attempts:
        question = await user_questions.get(user_id)
        if question:
            await display_question(ctx, question)
        else:
            await ctx.send("You don't have an active question. Use `!sql` to get a new question.")
    else:
        await ctx.send("You've used all your attempts for this question. Use `!sql` to get a new question.")

@bot.command()
async def leaderboard(ctx):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                top_users = await conn.fetch('''
                    SELECT u.username, l.points 
                    FROM leaderboard l
                    JOIN users u ON l.user_id = u.user_id
                    ORDER BY l.points DESC LIMIT 10
                ''')
        
        if top_users:
            headers = ["Rank", "User", "Points"]
            data = [(i, user['username'], user['points']) for i, user in enumerate(top_users, 1)]
            table = create_discord_table(headers, data)
            await ctx.send(f"🏆 Top 10 Leaderboard 🏆\n{table}")
        else:
            await ctx.send("No users on the leaderboard yet. Start answering questions to climb the ranks!")
    except Exception as e:
        logging.error(f"Error in leaderboard command: {e}")
        await ctx.send("An error occurred while fetching the leaderboard. Please try again later.")

@bot.command()
async def weekly_heroes(ctx):
    await user_last_active.set(ctx.author.id, datetime.now(timezone.utc))  # Add this line
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                week_start = await get_week_start()
                top_users = await conn.fetch('''
                    SELECT u.user_id, u.username, COUNT(*) as submissions, SUM(us.points) as points
                    FROM user_submissions us
                    JOIN users u ON us.user_id = u.user_id
                    WHERE us.submitted_at >= $1
                    GROUP BY u.user_id, u.username
                    ORDER BY submissions DESC, points DESC
                    LIMIT 10
                ''', week_start)
        
        if top_users:
            headers = ["Rank", "User", "Submissions", "Points"]
            data = []
            for i, user in enumerate(top_users, 1):
                data.append((i, user['username'], user['submissions'], user['points']))
            table = create_discord_table(headers, data)
            await ctx.send(f"🦸 Weekly Heroes \n{table}")
        else:
            await ctx.send("No submissions this week yet. Be the first hero!")
    except Exception as e:
        logging.error(f"Error in weekly_heroes command: {e}")
        await ctx.send("An error occurred while fetching the weekly heroes. Please try again later.")

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

@bot.command()
async def hint(ctx):
    user_id = ctx.author.id
    question = await user_questions.get(user_id)
    if question and question['hint']:
        await ctx.send(f"💡 Hint: {question['hint']}\n"
                       f"Use this wisdom wisely, young SQL padawan! 🧘‍♂️✨")
    else:
        await ctx.send("🤔 Hmm... No hint available for this question.\n"
                       "Time to put on your thinking cap! 🧢💭")

@bot.command()
async def list_categories(ctx):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                categories = await conn.fetch('''
                    SELECT DISTINCT category FROM questions
                    WHERE category IS NOT NULL
                    ORDER BY category
                ''')
        
        if categories:
            category_list = ", ".join([f"📁 {cat['category']}" for cat in categories])
            await ctx.send(f"🗂️ Available SQL Categories ️\n\n{category_list}\n\n"
                           f"Choose your path and conquer the SQL realm! 🏆")
        else:
            await ctx.send("🕵️‍♂️ Hmm... It seems our category list is on vacation.\n"
                           "Check back later for exciting SQL adventures! 🌴")
    except Exception as e:
        logging.error(f"Error in list_categories command: {e}")
        await ctx.send("⚠️ Oops! Our category finder is taking a coffee break.\n"
                       "Please try again later when it's caffeinated! ☕")

@tasks.loop(time=time(hour=23, minute=30))  # 11:30 PM IST
async def daily_question():
    global current_question
    try:
        current_question = await get_question()
        if current_question:
            points = {'easy': 60, 'medium': 80, 'hard': 120}.get(current_question['difficulty'], 0)
            for channel_id in CHANNEL_IDS:
                channel = bot.get_channel(channel_id)
                if channel:
                    await channel.send(f"Question ID: {current_question['id']}")
                    await channel.send(f"Daily SQL Question ({current_question['difficulty'].upper()}, worth {points} points):\n\n{current_question['question']}\n\nDataset:\n```\n{current_question['datasets']}\n```\n\nUse `!submit` followed by your SQL query to answer!\n\nIf you want to go to a previous question, use the `!question <id>` command with the desired question ID.")
        else:
            for channel_id in CHANNEL_IDS:
                channel = bot.get_channel(channel_id)
                if channel:
                    await channel.send("Sorry, no questions available for today's daily question.")
    except Exception as e:
        logging.error(f"Error in daily_question task: {e}")
# keeping it
@bot.command()
@db_connection_required()
async def my_stats(ctx):
    user_id = ctx.author.id
    async with DB_SEMAPHORE:
        async with bot.db.acquire() as conn:
            stats = await conn.fetchrow('''
                SELECT 
                    COUNT(*) as total_questions,
                    SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) as correct_answers,
                    AVG(points) as avg_points,
                    SUM(points) as total_points
                FROM user_submissions
                WHERE user_id = $1
            ''', user_id)
    
    if stats and stats['total_questions'] > 0:
        success_rate = (stats['correct_answers'] / stats['total_questions']) * 100
        await ctx.send(f"📊 Your SQL Journey Stats 📊\n\n"
                       f"🔢 Total Questions: {stats['total_questions']}\n"
                       f"✅ Correct Answers: {stats['correct_answers']}\n"
                       f"📈 Success Rate: {success_rate:.2f}%\n"
                       f"⭐ Average Points: {stats['avg_points']:.2f}\n"
                       f"💰 Total Points: {stats['total_points']}\n\n"
                       f"🌟 Keep coding and climbing the ranks! 🚀\n"
                       f"Remember, every query makes you stronger! 💪")
    else:
        await ctx.send("🚀 Your SQL Adventure Awaits! 🚀\n\n"
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
    print("Bot is ready.")
    if not hasattr(bot, 'db'):
        logging.error("Database connection not established. Shutting down.")
        await bot.close()

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
    `!report <question_id> <feedback>`: Report an issue with a question
    `!top_10`: View the current leaderboard
    `!weekly_heroes`: View this week's top performers
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

@bot.command()
async def leaderboard(ctx):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                top_users = await conn.fetch('''
                    SELECT u.username, l.points 
                    FROM leaderboard l
                    JOIN users u ON l.user_id = u.user_id
                    ORDER BY l.points DESC LIMIT 10
                ''')
        
        if top_users:
            headers = ["Rank", "User", "Points"]
            data = [(i, user['username'], user['points']) for i, user in enumerate(top_users, 1)]
            table = create_discord_table(headers, data)
            await ctx.send(f"🏆 Top 10 Leaderboard 🏆\n{table}")
        else:
            await ctx.send("No users on the leaderboard yet. Start answering questions to climb the ranks!")
    except Exception as e:
        logging.error(f"Error in leaderboard command: {e}")
        await ctx.send("An error occurred while fetching the leaderboard. Please try again later.")

@bot.command()
async def weekly_heroes(ctx):
    await user_last_active.set(ctx.author.id, datetime.now(timezone.utc))  # Add this line
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                week_start = await get_week_start()
                top_users = await conn.fetch('''
                    SELECT u.user_id, u.username, COUNT(*) as submissions, SUM(us.points) as points
                    FROM user_submissions us
                    JOIN users u ON us.user_id = u.user_id
                    WHERE us.submitted_at >= $1
                    GROUP BY u.user_id, u.username
                    ORDER BY submissions DESC, points DESC
                    LIMIT 10
                ''', week_start)
        
        if top_users:
            headers = ["Rank", "User", "Submissions", "Points"]
            data = []
            for i, user in enumerate(top_users, 1):
                data.append((i, user['username'], user['submissions'], user['points']))
            table = create_discord_table(headers, data)
            await ctx.send(f"🦸 Weekly Heroes \n{table}")
        else:
            await ctx.send("No submissions this week yet. Be the first hero!")
    except Exception as e:
        logging.error(f"Error in weekly_heroes command: {e}")
        await ctx.send("An error occurred while fetching the weekly heroes. Please try again later.")

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

@tasks.loop(time=time(hour=3, minute=30))  # 3:30 AM IST (10 PM UTC)
async def update_leaderboard():
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                top_users = await conn.fetch('''
                    SELECT u.username, l.points 
                    FROM leaderboard l
                    JOIN users u ON l.user_id = u.user_id
                    ORDER BY l.points DESC LIMIT 10
                ''')
        
        if top_users:
            headers = ["Rank", "User", "Points"]
            data = [(i, user['username'], user['points']) for i, user in enumerate(top_users, 1)]
            table = create_discord_table(headers, data)
            for channel_id in CHANNEL_IDS:
                channel = bot.get_channel(channel_id)
                if channel:
                    await channel.send(f"🏆 Daily Top 10 Leaderboard 🏆\n{table}")
    except Exception as e:
        logging.error(f"Error in update_leaderboard task: {e}")

@tasks.loop(time=time(hour=17, minute=30))  # 5:30 PM IST
async def daily_challenge():
    try:
        question = await get_question()
        if question:
            challenge_message = (
                "🌟 Daily SQL Challenge 🌟\n\n"
                f"Here's today's challenge (worth 150 points):\n\n"
                f"{question['question']}\n\n"
                f"Dataset:\n```\n{question['datasets']}\n```\n\n"
                "⏳ You have 4 hours to submit your answer!\n"
                "Use `!submit_challenge` followed by your SQL query to answer."
            )
            for channel_id in CHANNEL_IDS:
                channel = bot.get_channel(channel_id)
                if channel:
                    await channel.send(challenge_message)
    except Exception as e:
        logging.error(f"Error in daily_challenge task: {e}")

@tasks.loop(time=time(hour=21, minute=30))  # 9:30 PM IST
async def challenge_time_over():
    try:
        challenge_over_message = (
            "🕒 Daily SQL Challenge Time Over 🕒\n\n"
            "The time for today's challenge has ended. "
            "Don't worry if you missed it – a new challenge will be posted tomorrow!\n"
            "Keep practicing and improving your SQL skills! 💪"
        )
        for channel_id in CHANNEL_IDS:
            channel = bot.get_channel(channel_id)
            if channel:
                await channel.send(challenge_over_message)
    except Exception as e:
        logging.error(f"Error in challenge_time_over task: {e}")

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
    global current_question
    await asyncio.sleep(time_limit * 60)  # Convert minutes to seconds
    if current_question and current_question['id'] == question_id:
        await ctx.send(f"Time's up! The question (ID: {question_id}) has expired. Use `!question` to get a new question.")
        current_question = None

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
            await ctx.send(f"Congratulations! You've earned new achievements: {', '.join(new_achievements)}")
    except Exception as e:
        logging.error(f"Error in update_user_achievements: {e}")
        # Don't send an error message to the user for this internal error

async def ensure_tables_exist():
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                await conn.execute('''
                    -- Other table creations...

                    CREATE TABLE IF NOT EXISTS user_submissions (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        question_id INT,
                        is_correct BOOLEAN,
                        points INT,
                        submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    -- Add is_correct column if it doesn't exist
                    DO $$
                    BEGIN
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                       WHERE table_name='user_submissions' AND column_name='is_correct') THEN
                            ALTER TABLE user_submissions ADD COLUMN is_correct BOOLEAN;
                        END IF;
                    END $$;
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

async def get_user_stats(user_id):
    async with DB_SEMAPHORE:
        async with bot.db.acquire() as conn:
            stats = await conn.fetchrow('''
                SELECT 
                    COUNT(*) as total_answers,
                    SUM(CASE WHEN points > 0 THEN 1 ELSE 0 END) as correct_answers,
                    SUM(points) as total_points
                FROM user_submissions
                WHERE user_id = $1
            ''', user_id)
    return stats

async def post_achievement_announcement(user_id, new_achievements):
    user = await bot.fetch_user(user_id)
    stats = await get_user_stats(user_id)
    
    announcement = (
        f"🎉 Congratulations to {user.mention}! 🎉\n\n"
        f"They've just earned new achievement{'s' if len(new_achievements) > 1 else ''}:\n"
        f"{', '.join(new_achievements)}\n\n"
        f"📊 User Stats:\n"
        f"Total Questions Answered: {stats['total_answers']}\n"
        f"Correct Answers: {stats['correct_answers']}\n"
        f"Total Points: {stats['total_points']}\n\n"
        f"Keep up the great work! 💪🚀"
    )
    
    for channel_id in CHANNEL_IDS:
        channel = bot.get_channel(channel_id)
        if channel:
            await channel.send(announcement)

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
        # Task logic
        logging.info("Daily task completed successfully")
    except Exception as e:
        logging.error(f"Error in daily task: {e}", exc_info=True)

@daily_task.error
async def daily_task_error(error):
    logging.error(f"Unhandled error in daily task: {error}", exc_info=True)

async def calculate_points(user_id, is_correct, difficulty):
    base_points = {'easy': 60, 'medium': 80, 'hard': 120}.get(difficulty, 0)
    if is_correct:
        streak = await get_user_streak(user_id)
        streak_bonus = min(streak * 5, 50)  # Cap streak bonus at 50 points
        return base_points + streak_bonus
    else:
        return -20  # Deduct 20 points for incorrect answers

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
    await user_last_active.set(user_id, datetime.now(timezone.utc))  # Add this line
    today = get_ist_time().date()
    daily_points = await get_daily_points(user_id, today)
    daily_submissions = await get_daily_submissions(user_id, today)
    
    await ctx.send(f"📊 Your Daily Progress 📊\n"
                   f"Points earned today: {daily_points}\n"
                   f"Questions attempted: {daily_submissions}\n"
                   f"Keep up the great work! 💪")

@bot.command()
async def weekly_progress(ctx):
    user_id = ctx.author.id
    await user_last_active.set(user_id, datetime.now(timezone.utc))  # Add this line
    weekly_points = await get_weekly_points(user_id)
    
    await ctx.send(f"🗓️ Your Weekly Progress 🗓️\n"
                   f"Points earned this week: {weekly_points}\n"
                   f"You're making great strides! 🚀")

                   
# @bot.command()
# async def help(ctx):
#     help_text = """
#     Available commands:
#     `!sql`: Get a random SQL question
#     `!easy`, `!medium`, `!hard`: Get a question of specific difficulty
#     `!topic`: List all available topics or get a question on a specific SQL topic
#     `!company`: List all available companies or practice questions from a specific company
#     `!submit <answer>`: Submit your answer to the current question
#     `!set_difficulty <difficulty>`: Set your preferred question difficulty
#     `!sql_battle`: Start an SQL battle with other users
#     `!my_stats`: View your overall statistics
#     `!leaderboard`: View the top 10 users
#     `!weekly_heroes`: View this week's top performers
#     `!submit_question`: Submit a new question for review

#     For more detailed help on each command, use `!help <command_name>`.
#     """
#     await ctx.send(help_text)

@bot.command()
@db_connection_required()
async def my_stats(ctx):
    user_id = ctx.author.id
    async with DB_SEMAPHORE:
        async with bot.db.acquire() as conn:
            stats = await conn.fetchrow('''
                SELECT 
                    COUNT(*) as total_questions,
                    SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) as correct_answers,
                    AVG(points) as avg_points,
                    SUM(points) as total_points
                FROM user_submissions
                WHERE user_id = $1
            ''', user_id)
    
    if stats and stats['total_questions'] > 0:
        success_rate = (stats['correct_answers'] / stats['total_questions']) * 100
        await ctx.send(f"📊 Your SQL Journey Stats 📊\n\n"
                       f"🔢 Total Questions: {stats['total_questions']}\n"
                       f"✅ Correct Answers: {stats['correct_answers']}\n"
                       f"📈 Success Rate: {success_rate:.2f}%\n"
                       f"⭐ Average Points: {stats['avg_points']:.2f}\n"
                       f"💰 Total Points: {stats['total_points']}\n\n"
                       f"🌟 Keep coding and climbing the ranks! 🚀\n"
                       f"Remember, every query makes you stronger! 💪")
    else:
        await ctx.send("🚀 Your SQL Adventure Awaits! 🚀\n\n"
                       "You haven't answered any questions yet. Let's change that!\n"
                       "Use `!sql` to get your first question and start your journey.\n\n"
                       "Remember, every SQL master started as a beginner. Your coding adventure begins now! 💪✨")



@tasks.loop(minutes=30)
async def cleanup_inactive_users():
    # This function only cleans up temporary in-memory data
    # It does not affect any user data stored in the database
    current_time = datetime.now(timezone.utc)
    inactive_threshold = timedelta(hours=1)
    
    async def is_user_active(user_id):
        last_active = await user_last_active.get(user_id)
        if last_active is None:
            return False
        return (current_time - last_active) < inactive_threshold
    
    # Use a list to store keys to avoid modifying the dictionary during iteration
    user_ids = list(user_questions._dict.keys())
    for user_id in user_ids:
        if not await is_user_active(user_id):
            await user_questions.pop(user_id, None)
            await user_attempts.pop(user_id, None)
            await user_skips.pop(user_id, None)
            await user_last_active.pop(user_id, None)
    
    logging.info(f"Cleaned up inactive users. Active users: {len(user_questions._dict)}")

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

                # Check if user has attempted all questions from this company
                attempted = await conn.fetchval("""
                    SELECT COUNT(*) FROM user_submissions
                    JOIN questions ON user_submissions.question_id = questions.id
                    WHERE user_submissions.user_id = $1 AND LOWER(questions.company) = $2
                """, user_id, best_match)

                total = await conn.fetchval("SELECT COUNT(*) FROM questions WHERE LOWER(company) = $1", best_match)

                if attempted == total:
                    await ctx.send(f"You've attempted all questions from {best_match.title()}. Try another company:")
                    await list_companies(ctx)
                    return

                question = await get_question(company=best_match, user_id=user_id)
                if question:
                    await user_questions.set(user_id, question)
                    await display_question(ctx, question)
                else:
                    await ctx.send(f"Sorry, no more questions available for the company '{best_match.title()}' at the moment.")
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
async def update_user_stats(user_id, points, is_correct):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                await conn.execute('''
                    INSERT INTO user_submissions (user_id, points, is_correct)
                    VALUES ($1, $2, $3)
                ''', user_id, points, is_correct)
        
        # Update weekly points
        await update_weekly_points(user_id, points)
        
    except Exception as e:
        logging.error(f"Error updating user stats: {e}")
        raise

async def update_user_submissions(user_id, question_id, is_correct, points):
    try:
        async with DB_SEMAPHORE:
            async with bot.db.acquire() as conn:
                await conn.execute('''
                    INSERT INTO user_submissions (user_id, question_id, is_correct, points)
                    VALUES ($1, $2, $3, $4)
                ''', user_id, question_id, is_correct, points)
    except Exception as e:
        logging.error(f"Error updating user submissions: {e}")
        raise

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
