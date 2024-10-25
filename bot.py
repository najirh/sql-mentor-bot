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

# Setup
logging.basicConfig(level=logging.INFO)
load_dotenv()

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents, help_command=None)

# Global dictionaries to track user-specific data
user_questions = {}
user_attempts = {}
user_skips = {}

CHANNEL_IDS = [int(id.strip()) for id in os.getenv('CHANNEL_ID', '').split(',') if id.strip()]

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
        
        bot.db = await asyncpg.create_pool(database_url, ssl='require')
        logging.info("Database connection established")
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        raise

async def ensure_user_exists(user_id, username):
    try:
        async with bot.db.acquire() as conn:
            await conn.execute('''
                INSERT INTO users (user_id, username)
                VALUES ($1, $2)
                ON CONFLICT (user_id) DO NOTHING
            ''', user_id, username)
    except Exception as e:
        logging.error(f"Error ensuring user exists: {e}")
        raise

async def get_question(difficulty=None, user_id=None):
    try:
        logging.info(f"Fetching question for user_id: {user_id}, difficulty: {difficulty}")
        async with bot.db.acquire() as conn:
            query = '''
                SELECT q.* FROM questions q
                LEFT JOIN user_submissions us ON q.id = us.question_id AND us.user_id = $1
                LEFT JOIN reports r ON q.id = r.question_id
                WHERE us.question_id IS NULL 
                AND r.question_id IS NULL
                AND ($2::VARCHAR IS NULL OR q.difficulty = $2::VARCHAR)
                ORDER BY RANDOM() LIMIT 1
            '''
            logging.info(f"Executing query: {query}")
            question = await conn.fetchrow(query, user_id, difficulty)
        logging.info(f"Fetched question: {question}")
        return question
    except Exception as e:
        logging.error(f"Error fetching question: {e}")
        return None

async def get_week_start():
    return datetime.now().date() - timedelta(days=datetime.now().weekday())

async def update_weekly_points(user_id, points):
    try:
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
        async with bot.db.acquire() as conn:
            points = await conn.fetchval('''
                SELECT COALESCE(points, 0)
                FROM daily_points
                WHERE user_id = $1 AND date = $2
            ''', user_id, date)
        return points
    except Exception as e:
        logging.error(f"Error getting daily points: {e}")
        return 0

async def update_daily_points(user_id, date, points):
    try:
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
    points = {'easy': 60, 'medium': 80, 'hard': 120}.get(question['difficulty'], 0)
    time_limit = {'easy': 10, 'medium': 20, 'hard': 30}.get(question['difficulty'], 15)  # in minutes
    await ctx.send(f"Question ID: {question['id']}")
    await ctx.send(f"Here's a {question['difficulty'].upper()} question (worth {points} points):\n\n{question['question']}\n\nDataset:\n```\n{question['datasets']}\n```\n\n⏳ You have {time_limit} minutes to answer this question.\nTo submit your answer, use the `!submit` command followed by your SQL query.")

async def check_daily_limit(ctx, user_id):
    today = get_ist_time().date()
    daily_points = await get_daily_points(user_id, today)
    daily_submissions = await get_daily_submissions(user_id, today)
    if daily_points <= -50 or daily_submissions >= 10:
        await ctx.send("You've reached the daily limit. Please try again tomorrow! 🌙")
        return False
    return True

async def get_daily_submissions(user_id, date):
    try:
        async with bot.db.acquire() as conn:
            submissions = await conn.fetchval('''
                SELECT COUNT(*) FROM user_submissions
                WHERE user_id = $1 AND DATE(submitted_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') = $2
            ''', user_id, date)
        return submissions
    except Exception as e:
        logging.error(f"Error getting daily submissions: {e}")
        return 0

@bot.command(name='sql')
@cooldown(1, 60, BucketType.user)
async def sql_question_command(ctx):
    user_id = ctx.author.id
    username = str(ctx.author)
    try:
        await ensure_user_exists(user_id, username)
        logging.info(f"User {user_id} ({username}) requested a SQL question")

        if not await check_daily_limit(ctx, user_id):
            logging.info(f"User {user_id} has reached their daily limit")
            return

        question = await get_question(user_id=user_id)
        if question:
            user_questions[user_id] = question
            logging.info(f"Stored question for user {user_id}: {question['id']}")
            await display_question(ctx, question)
        else:
            logging.warning(f"No questions available for user {user_id}")
            await ctx.send("Sorry, no questions available at the moment.")
    except Exception as e:
        logging.error(f"Error in sql command for user {user_id}: {e}", exc_info=True)
        await ctx.send("An error occurred while fetching a question. Please try again later.")

@sql_question_command.error
async def sql_question_command_error(ctx, error):
    if isinstance(error, commands.CommandOnCooldown):
        await ctx.send(f"This command is on cooldown. Try again in {error.retry_after:.2f} seconds.")

@bot.command()
async def submit(ctx, *, answer):
    user_id = ctx.author.id
    if user_id not in user_questions:
        await ctx.send("📚 There's no active question. Use `!sql` to get a new challenge! 🚀")
        return

    question = user_questions[user_id]
    logging.info(f"Retrieved question for user {user_id}: {question['id']}")
    if similar(answer.lower(), question['answer'].lower()) >= 0.8:
        points = {'easy': 60, 'medium': 80, 'hard': 120}.get(question['difficulty'], 0)
        await ctx.send(f"🎉 Brilliant! You've cracked the code! 🧠💡\n"
                       f"You've earned {points} points! 🌟\n"
                       f"Keep up the fantastic work, SQL wizard! 🧙‍♂️✨")
        user_attempts[user_id] = 0  # Reset attempts
        
        try:
            new_achievements, all_achievements = await check_achievements(user_id)
            if new_achievements:
                await post_achievement_announcement(user_id, new_achievements)
            
            if all_achievements:
                await ctx.send(f"Your current achievements: {', '.join(all_achievements)}")
        except Exception as e:
            logging.error(f"Error processing achievements: {e}")
    else:
        points = -10  # Reduce points for incorrect answers
        user_attempts[user_id] = user_attempts.get(user_id, 0) + 1
        if user_attempts[user_id] == 1:
            await ctx.send(f"❌ Oops! Not quite there yet, but don't give up! 💪\n"
                           f"You've lost {abs(points)} points. 📉\n"
                           f"You have one more try! Use `!try_again` to attempt once more. 🔄\n"
                           f"To get a hint, type `!hint`. You've got this! ")
        else:
            await ctx.send(f"❌ Sorry, that's not correct. You've used all your attempts.\n"
                           f"You've lost {abs(points)} points. 📉\n"
                           f"Keep practicing and you'll improve!")

    today = get_ist_time().date()
    async with bot.db.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_submissions (user_id, question_id, submitted_at, points)
            VALUES ($1, $2, $3, $4)
        ''', user_id, question['id'], get_ist_time(), points)
        
        await update_weekly_points(user_id, points)
        await update_daily_points(user_id, today, points)

    del user_questions[user_id]  # Remove the question after submission

@bot.command()
async def my_stats(ctx):
    user_id = ctx.author.id
    async with bot.db.acquire() as conn:
        stats = await conn.fetchrow('''
            SELECT 
                COUNT(*) as total_questions,
                SUM(CASE WHEN points > 0 THEN 1 ELSE 0 END) as correct_answers,
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
                       f" Success Rate: {success_rate:.2f}%\n"
                       f"⭐ Average Points: {stats['avg_points']:.2f}\n"
                       f"💰 Total Points: {stats['total_points']}\n\n"
                       f"🌟 Keep coding and climbing the ranks! 🚀\n"
                       f"Remember, every query makes you stronger! 💪")
    else:
        await ctx.send("📚 Your SQL Adventure Awaits! 🚀\n\n"
                       "You haven't answered any questions yet. Let's change that!\n"
                       "Use `!sql` to get your first question and start your journey.\n\n"
                       "Remember, every SQL master started as a beginner. Your coding adventure begins now! 💪✨")

@tasks.loop(time=time(hour=0))  # Midnight UTC
async def reset_daily_points():
    try:
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
    print(f'{bot.user} has connected to Discord!')
    try:
        await wait_for_db()
        await ensure_tables_exist()
        
        daily_task.start()
        daily_challenge.start()
        update_leaderboard.start()
        challenge_time_over.start()
    except Exception as e:
        logging.error(f"Error during startup: {e}", exc_info=True)
        await bot.close()

@bot.event
async def on_error(event, *args, **kwargs):
    logging.error(f"An error occurred in event {event}", exc_info=True)

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        await ctx.send("Invalid command. Use `!help` to see available commands.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("You're missing a required argument. Check `!help` for command usage.")
    else:
        logging.error(f"Unhandled error: {error}", exc_info=True)
        await ctx.send("An unexpected error occurred. Please try again later or contact the administrator.")

@bot.event
async def on_disconnect():
    print("Bot disconnected from Discord")

@bot.command(name='help')
async def help_command(ctx):
    help_message = """
    Available commands:
    `!help` - Display this help message
    `!sql` - Get a random SQL question
    `!easy` - Get an easy SQL question
    `!medium` - Get a medium SQL question
    `!hard` - Get a hard SQL question
    `!topic <topic_name>` - Get a question based on a specific topic
    `!skip` - Skip the current question (only one skip allowed per question)
    `!submit <answer>` - Submit your answer to the current question
    `!top_10` - Show top 10 users based on ranking
    `!weekly_heroes` - Show top 10 users based on weekly submissions and their current streak
    `!report <question_id> <feedback>` - Report a question if you think it's incorrect
    `!my_stats` - View your personal statistics
    `!sql_battle` - Start an SQL battle with other users
    `!set_difficulty <difficulty>` - Set your preferred question difficulty
    `!my_difficulty` - Check your current preferred difficulty
    `!rate_question <question_id> <rating>` - Rate a question (1-5 stars)
    `!question_stats <question_id>` - View statistics for a specific question
    `!my_achievements` - View your earned achievements

    To submit an answer, use the `!submit` command followed by your SQL query.
    For example: `!submit SELECT * FROM users WHERE age > 18`

    To report a question, use the `!report` command followed by the question ID and your feedback.
    For example: `!report 1 The question is ambiguous and needs clarification.`

    Good luck and happy coding!
    """
    await ctx.send(help_message)

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
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)
    try:
        question = await get_question(difficulty, user_id)
        if question:
            user_questions[user_id] = question
            await display_question(ctx, question)
        else:
            await ctx.send(f"Sorry, no {difficulty} questions available at the moment.")
    except Exception as e:
        logging.error(f"Error in {difficulty} command: {e}")
        await ctx.send("An error occurred while fetching a question. Please try again later.")

@bot.command()
async def question(ctx):
    user_id = ctx.author.id
    async with bot.db.acquire() as conn:
        difficulty = await conn.fetchval('''
            SELECT preferred_difficulty FROM user_preferences
            WHERE user_id = $1
        ''', user_id)
    
    question = await get_question(difficulty, user_id)
    if question:
        user_questions[user_id] = question
        await display_question(ctx, question)
    else:
        await ctx.send("Sorry, no questions available at the moment.")

@bot.command()
async def try_again(ctx):
    user_id = ctx.author.id
    if user_id in user_attempts and user_attempts[user_id] == 1:
        await ctx.send("You can try again. Please submit your new answer using the `!submit` command.")
    else:
        await ctx.send("You don't have any attempts left or there's no active question for you.")

@bot.command()
async def top_10(ctx):
    try:
        async with bot.db.acquire() as conn:
            top_users = await conn.fetch('SELECT user_id, points FROM leaderboard ORDER BY points DESC LIMIT 10')
        
        if top_users:
            headers = ["Rank", "User ID", "Points"]
            data = [(i, user['user_id'], user['points']) for i, user in enumerate(top_users, 1)]
            table = create_discord_table(headers, data)
            await ctx.send(f"🏆 Top 10 Leaderboard 🏆\n{table}")
        else:
            await ctx.send("No users on the leaderboard yet.")
    except Exception as e:
        logging.error(f"Error in top_10 command: {e}")
        await ctx.send("An error occurred while fetching the leaderboard. Please try again later.")

@bot.command()
async def weekly_heroes(ctx):
    try:
        async with bot.db.acquire() as conn:
            week_start = await get_week_start()
            top_users = await conn.fetch('''
                SELECT user_id, COUNT(*) as submissions, SUM(points) as points
                FROM user_submissions
                WHERE submitted_at >= $1
                GROUP BY user_id
                ORDER BY submissions DESC, points DESC
                LIMIT 10
            ''', week_start)
        
        if top_users:
            headers = ["Rank", "User ID", "Submissions", "Points", "Streak"]
            data = []
            for i, user in enumerate(top_users, 1):
                streak = await get_user_streak(user['user_id'])
                data.append((i, user['user_id'], user['submissions'], user['points'], f"{streak} days"))
            table = create_discord_table(headers, data)
            await ctx.send(f"🦸 Weekly Heroes 🦸\n{table}")
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
async def skip(ctx):
    user_id = ctx.author.id

    if user_id not in user_questions:
        await ctx.send("There's no active question. Use `!question` to get a question first.")
        return

    if user_id in user_skips and user_skips[user_id]:
        await ctx.send("You've already used your skip for this question. Please answer the current question or wait for the next one.")
        return

    try:
        new_question = await get_question(user_id=user_id)
        if new_question:
            user_questions[user_id] = new_question
            user_skips[user_id] = True
            await display_question(ctx, new_question)
            await ctx.send("Question skipped. Here's a new question for you.")
        else:
            await ctx.send("Sorry, no more questions available at the moment.")
    except Exception as e:
        logging.error(f"Error in skip command: {e}")
        await ctx.send("An error occurred while fetching a new question. Please try again later.")

@bot.command()
async def topic(ctx, *, topic_name):
    user_id = ctx.author.id
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)
    try:
        async with bot.db.acquire() as conn:
            question = await conn.fetchrow('''
                SELECT * FROM questions
                WHERE topic = $1
                ORDER BY RANDOM() LIMIT 1
            ''', topic_name)

        if question:
            await display_question(ctx, question)
        else:
            await ctx.send(f"No questions available for the topic '{topic_name}'.")
    except Exception as e:
        logging.error(f"Error in topic command: {e}")
        await ctx.send("An error occurred while fetching the question. Please try again later.")

@bot.command()
async def hint(ctx):
    user_id = ctx.author.id
    if user_id in user_questions and user_questions[user_id]['hint']:
        await ctx.send(f"💡 Hint: {user_questions[user_id]['hint']}\n"
                       f"Use this wisdom wisely, young SQL padawan! 🧘‍♂️✨")
    else:
        await ctx.send("🤔 Hmm... No hint available for this question.\n"
                       "Time to put on your thinking cap! 🧢💭")

@bot.command()
async def list_categories(ctx):
    try:
        async with bot.db.acquire() as conn:
            categories = await conn.fetch('''
                SELECT DISTINCT category FROM questions
                WHERE category IS NOT NULL
                ORDER BY category
            ''')
        
        if categories:
            category_list = ", ".join([f"📁 {cat['category']}" for cat in categories])
            await ctx.send(f"🗂️ Available SQL Categories 🗂️\n\n{category_list}\n\n"
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

@tasks.loop(time=time(hour=3, minute=30))  # 3:30 AM IST (10 PM UTC)
async def update_leaderboard():
    try:
        async with bot.db.acquire() as conn:
            top_users = await conn.fetch('SELECT user_id, points FROM leaderboard ORDER BY points DESC LIMIT 10')
        
        if top_users:
            headers = ["Rank", "User ID", "Points"]
            data = [(i, user['user_id'], user['points']) for i, user in enumerate(top_users, 1)]
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
    async with bot.db.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_challenges (user_id, total_questions, correct_answers, time_taken)
            VALUES ($1, $2, $3, $4)
        ''', user_id, num_questions, correct_answers, total_time)

@bot.command()
async def challenge_history(ctx):
    user_id = ctx.author.id
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
            await ctx.send(f"Date: {challenge['completed_at']}, Score: {challenge['correct_answers']}/{challenge['total_questions']}, Time: {challenge['time_taken']:.2f} minutes")
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
        await self.channel.send(f"🎉 {winner.mention} wins the SQL Battle! 🎉")

@bot.command()
async def sql_battle(ctx):
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
    valid_difficulties = ['easy', 'medium', 'hard']
    if difficulty.lower() not in valid_difficulties:
        await ctx.send(f"Invalid difficulty. Please choose from: {', '.join(valid_difficulties)}")
        return

    user_id = ctx.author.id
    async with bot.db.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_preferences (user_id, preferred_difficulty)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET preferred_difficulty = $2
        ''', user_id, difficulty.lower())
    
    await ctx.send(f"Your preferred difficulty has been set to {difficulty}.")

@bot.command()
async def my_difficulty(ctx):
    user_id = ctx.author.id
    async with bot.db.acquire() as conn:
        difficulty = await conn.fetchval('''
            SELECT preferred_difficulty FROM user_preferences
            WHERE user_id = $1
        ''', user_id)
    
    if difficulty:
        await ctx.send(f"Your current preferred difficulty is: {difficulty}")
    else:
        await ctx.send("You haven't set a preferred difficulty yet. Use !set_difficulty to set one.")

@bot.command()
async def rate_question(ctx, question_id: int, rating: int):
    if rating < 1 or rating > 5:
        await ctx.send("Please rate the question from 1 to 5.")
        return

    user_id = ctx.author.id
    async with bot.db.acquire() as conn:
        await conn.execute('''
            INSERT INTO question_ratings (user_id, question_id, rating)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id, question_id) DO UPDATE SET rating = $3
        ''', user_id, question_id, rating)
    
    await ctx.send(f"Thank you for rating question {question_id}!")

@bot.command()
async def question_stats(ctx, question_id: int):
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

async def get_user_streak(user_id):
    async with bot.db.acquire() as conn:
        submissions = await conn.fetch('''
            SELECT submitted_at::date, points
            FROM user_submissions
            WHERE user_id = $1
            ORDER BY submitted_at DESC
        ''', user_id)
        
        streak = 0
        last_date = None
        for submission in submissions:
            if last_date is None:
                last_date = submission['submitted_at']
                if submission['points'] > 0:
                    streak = 1
            elif (last_date - submission['submitted_at']).days == 1:
                if submission['points'] > 0:
                    streak += 1
                last_date = submission['submitted_at']
            else:
                break
    return streak

async def ensure_tables_exist():
    try:
        async with bot.db.acquire() as conn:
            await conn.execute('''
                -- Create tables if they don't exist
                CREATE TABLE IF NOT EXISTS questions (
                    id SERIAL PRIMARY KEY,
                    question TEXT NOT NULL,
                    answer TEXT NOT NULL,
                    datasets TEXT,
                    difficulty VARCHAR(25) NOT NULL,
                    hint TEXT,
                    topic TEXT
                );

                CREATE TABLE IF NOT EXISTS reports (
                    id SERIAL PRIMARY KEY,
                    reported_by BIGINT NOT NULL,
                    question_id INTEGER,
                    remarks TEXT NOT NULL,
                    reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS leaderboard (
                    user_id BIGINT PRIMARY KEY,
                    points INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS user_submissions (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    question_id INTEGER,
                    points INTEGER NOT NULL,
                    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS weekly_points (
                    user_id BIGINT NOT NULL,
                    points INTEGER DEFAULT 0,
                    week_start DATE NOT NULL,
                    PRIMARY KEY (user_id, week_start)
                );

                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT
                );

                CREATE TABLE IF NOT EXISTS user_challenges (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    total_questions INTEGER NOT NULL,
                    correct_answers INTEGER NOT NULL,
                    time_taken FLOAT NOT NULL,
                    completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS user_preferences (
                    user_id BIGINT PRIMARY KEY,
                    preferred_difficulty VARCHAR(10)
                );

                CREATE TABLE IF NOT EXISTS question_ratings (
                    user_id BIGINT,
                    question_id INTEGER,
                    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
                    PRIMARY KEY (user_id, question_id)
                );

                CREATE TABLE IF NOT EXISTS user_achievements (
                    user_id BIGINT,
                    achievement VARCHAR(50),
                    earned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, achievement)
                );

                CREATE TABLE IF NOT EXISTS daily_points (
                    user_id BIGINT,
                    date DATE,
                    points INT,
                    PRIMARY KEY (user_id, date)
                );
            ''')
    except Exception as e:
        logging.error(f"Error ensuring tables exist: {e}")
        raise

async def wait_for_db():
    max_retries = 10
    retry_interval = 5  # seconds

    for attempt in range(max_retries):
        try:
            await create_db_pool()
            print("Successfully connected to the database")
            return
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries}: Failed to connect to the database: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_interval} seconds...")
                await asyncio.sleep(retry_interval)
            else:
                raise Exception("Failed to connect to the database after multiple attempts")

async def graceful_shutdown():
    print("Shutting down gracefully...")
    if hasattr(bot, 'db'):
        await bot.db.close()
    await bot.close()

async def get_user_stats(user_id):
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
    try:
        async with bot.db.acquire() as conn:
            result = await conn.fetchval("SELECT 1")
        if result == 1:
            await ctx.send("Database connection is working!")
        else:
            await ctx.send("Database connection test failed.")
    except Exception as e:
        logging.error(f"Error checking database connection: {e}")
        await ctx.send("An error occurred while checking the database connection.")

@tasks.loop(hours=24)
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
    streak = await get_user_streak(user_id)
    bonus = min(streak * 5, 50)  # 5 points per day, up to 50
    return base_points + bonus if is_correct else -20

async def get_topic_question(ctx, topic):
    user_id = ctx.author.id
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)
    try:
        question = await get_question(topic=topic, user_id=user_id)
        if question:
            user_questions[user_id] = question
            await display_question(ctx, question)
        else:
            await ctx.send(f"Sorry, no questions available for the topic '{topic}' at the moment.")
    except Exception as e:
        logging.error(f"Error in topic question command: {e}")
        await ctx.send("An error occurred while fetching a question. Please try again later.")

def get_ist_time():
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))

if __name__ == "__main__":
    required_vars = ['DATABASE_URL', 'DISCORD_TOKEN', 'CHANNEL_ID']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        for var in required_vars:
            print(f"{var}: {os.getenv(var)}")
        raise ValueError("Missing required environment variables")
    bot.run(os.getenv('DISCORD_TOKEN'))
