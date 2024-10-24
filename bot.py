import os
import discord
from discord.ext import commands, tasks
import asyncpg
from dotenv import load_dotenv
from datetime import datetime, time, timedelta
import logging
import asyncio
from aiohttp import web
import aiohttp
from difflib import SequenceMatcher
import random

# Setup
logging.basicConfig(level=logging.INFO)
load_dotenv()

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents, help_command=None)

current_question = None
user_attempts = {}
user_skips = {}

CHANNEL_IDS = [int(id.strip()) for id in os.getenv('CHANNEL_ID').split(',')]

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
        bot.db = await asyncpg.create_pool(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        logging.info("Database connection established")
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")

async def ensure_user_exists(user_id, username):
    async with bot.db.acquire() as conn:
        await conn.execute('''
            INSERT INTO users (user_id, username)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO NOTHING
        ''', user_id, username)

async def get_question(difficulty=None, user_id=None):
    try:
        logging.info(f"Fetching question for user_id: {user_id}, difficulty: {difficulty}")
        async with bot.db.acquire() as conn:
            question = await conn.fetchrow('''
                SELECT q.* FROM questions q
                LEFT JOIN user_submissions us ON q.id = us.question_id AND us.user_id = $1
                WHERE us.question_id IS NULL AND ($2::VARCHAR IS NULL OR q.difficulty = $2::VARCHAR)
                ORDER BY RANDOM() LIMIT 1
            ''', user_id, difficulty)
        logging.info(f"Fetched question: {question}")
        return question
    except Exception as e:
        logging.error(f"Error fetching question: {e}")
        return None

async def get_week_start():
    return datetime.now().date() - timedelta(days=datetime.now().weekday())

async def update_weekly_points(user_id, points):
    async with bot.db.acquire() as conn:
        week_start = await get_week_start()
        await conn.execute('''
            INSERT INTO weekly_points (user_id, points, week_start)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id, week_start)
            DO UPDATE SET points = weekly_points.points + EXCLUDED.points
        ''', user_id, points, week_start)

async def get_weekly_points(user_id):
    async with bot.db.acquire() as conn:
        week_start = await get_week_start()
        points = await conn.fetchval('''
            SELECT COALESCE(points, 0)
            FROM weekly_points
            WHERE user_id = $1 AND week_start = $2
        ''', user_id, week_start)
    return points

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

async def display_question(ctx, question):
    points = {'easy': 60, 'medium': 80, 'hard': 120}.get(question['difficulty'], 0)
    await ctx.send(f"Question ID: {question['id']}")
    await ctx.send(f"Here's a {question['difficulty'].upper()} question (worth {points} points):\n\n{question['question']}\n\nDataset:\n```\n{question['datasets']}\n```\n\nTo submit your answer, use the `!submit` command followed by your SQL query.\n\nIf you want to skip this question, use the `!skip` command. You can only skip once per question.")

async def wait_for_db():
    max_retries = 10
    retry_interval = 5  # seconds

    for _ in range(max_retries):
        try:
            conn = await asyncpg.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                database=os.getenv('DB_NAME')
            )
            await conn.close()
            print("Successfully connected to the database")
            return
        except Exception as e:
            print(f"Failed to connect to the database: {e}")
            await asyncio.sleep(retry_interval)

    raise Exception("Failed to connect to the database after multiple attempts")

async def ensure_tables_exist():
    async with bot.db.acquire() as conn:
        await conn.execute('''
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
        ''')

        # Add the topic column if it doesn't exist
        await conn.execute('''
            ALTER TABLE questions
            ADD COLUMN IF NOT EXISTS topic TEXT;
        ''')

async def healthcheck(request):
    return web.Response(text="OK")

async def start_healthcheck():
    app = web.Application()
    app.router.add_get('/health', healthcheck)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()

async def keep_alive():
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('http://localhost:8080/health') as response:
                    if response.status == 200:
                        logging.info("Keep-alive ping successful.")
                    else:
                        logging.warning(f"Keep-alive ping failed with status: {response.status}")
        except Exception as e:
            logging.error(f"Error during keep-alive ping: {e}")
        
        await asyncio.sleep(300)  # Wait for 5 minutes before the next ping

@bot.event
async def on_ready():
    print(f'{bot.user} has connected to Discord!')
    
    await create_db_pool()
    await ensure_tables_exist()
    
    daily_question.start()
    update_leaderboard.start()
    post_weekly_heroes.start()

    for channel_id in CHANNEL_IDS:
        channel = bot.get_channel(channel_id)
        if channel:
            await channel.send("SQL Mentor is online!")
        else:
            print(f"Could not find channel with ID {channel_id}")

    await wait_for_db()
    await start_healthcheck()
    bot.loop.create_task(keep_alive())

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        await ctx.send("Invalid command. Use `!help` to see available commands.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("You're missing a required argument. Check `!help` for command usage.")
    else:
        logging.error(f"Unhandled error: {error}")
        await ctx.send("An error occurred while processing your command. Please try again later.")

@bot.command(name='help')
async def help_command(ctx):
    help_message = """
    Available commands:
    `!help` - Display this help message
    `!interview` - Get a random SQL question
    `!easy` - Get an easy SQL question
    `!medium` - Get a medium SQL question
    `!hard` - Get a hard SQL question
    `!topic <topic_name>` - Get a question based on a specific topic
    `!skip` - Skip the current question (only one skip allowed per question)
    `!my_scores` - Display your current score
    `!submit <answer>` - Submit your answer to the current question
    `!top_10` - Show top 10 users based on ranking
    `!weekly_heroes` - Show top 10 users based on weekly submissions and their current streak
    `!report <question_id> <feedback>` - Report a question if you think it's incorrect

    To submit an answer, use the `!submit` command followed by your SQL query.
    For example: `!submit SELECT * FROM users WHERE age > 18`

    To report a question, use the `!report` command followed by the question ID and your feedback.
    For example: `!report 1 The question is ambiguous and needs clarification.`

    Good luck and happy coding!
    """
    await ctx.send(help_message)

@bot.command()
async def interview(ctx):
    global current_question
    user_id = ctx.author.id
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)
    try:
        current_question = await get_question(user_id=user_id)
        if current_question:
            await display_question(ctx, current_question)
        else:
            await ctx.send("Sorry, no questions available at the moment.")
    except Exception as e:
        logging.error(f"Error in interview command: {e}")
        await ctx.send("An error occurred while fetching a question. Please try again later.")

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
    global current_question
    user_id = ctx.author.id
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)
    try:
        current_question = await get_question(difficulty, user_id)
        if current_question:
            await display_question(ctx, current_question)
        else:
            await ctx.send(f"Sorry, no {difficulty} questions available at the moment.")
    except Exception as e:
        logging.error(f"Error in {difficulty} command: {e}")
        await ctx.send("An error occurred while fetching a question. Please try again later.")

@bot.command()
async def question(ctx, question_id: int):
    global current_question
    try:
        async with bot.db.acquire() as conn:
            question = await conn.fetchrow('''
                SELECT q.* FROM questions q
                LEFT JOIN reports r ON q.id = r.question_id
                WHERE q.id = $1 AND r.question_id IS NULL
            ''', question_id)
        
        if question:
            current_question = question
            await display_question(ctx, question)
        else:
            await ctx.send(f"Sorry, the question with ID {question_id} is not available.")
    except Exception as e:
        logging.error(f"Error in question command: {e}")
        await ctx.send("An error occurred while fetching the question. Please try again later.")

@bot.command()
async def my_scores(ctx):
    user_id = ctx.author.id
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)
    try:
        async with bot.db.acquire() as conn:
            total_points = await conn.fetchval('SELECT points FROM leaderboard WHERE user_id = $1', user_id)
            weekly_points = await get_weekly_points(user_id)
            streak = await get_user_streak(user_id)
            
            if total_points is None:
                total_points = 0

            await ctx.send(f"Your scores:\nTotal points: {total_points}\nWeekly points: {weekly_points}\nCurrent streak: {streak} days")
    except Exception as e:
        logging.error(f"Error in my_scores command: {e}")
        await ctx.send("An error occurred while fetching your scores. Please try again later.")

@bot.command()
async def submit(ctx, *, answer):
    global current_question, user_attempts, user_skips
    user_id = ctx.author.id
    username = str(ctx.author)
    await ensure_user_exists(user_id, username)
    if not current_question:
        await ctx.send("There's no active question. Use `!interview` to get a question first.")
        return

    try:
        async with bot.db.acquire() as conn:
            # Normalize user answer: convert to single line and remove extra whitespace
            user_answer = ' '.join(answer.split()).strip().lower()
            # Normalize correct answer: convert to single line and remove extra whitespace
            correct_answer = ' '.join(current_question['answer'].split()).strip().lower()
            
            similarity = similar(user_answer, correct_answer)
            correct = similarity >= 0.65

            points = 0
            if correct:
                points = {'easy': 60, 'medium': 80, 'hard': 120}.get(current_question['difficulty'], 0)
                emoji = "🎉"
                result_message = "Correct!"
            else:
                points = -10
                emoji = "😢"
                result_message = "Sorry, that's not correct."

            # Insert into user_submissions
            await conn.execute('''
                INSERT INTO user_submissions (user_id, question_id, points)
                VALUES ($1, $2, $3)
            ''', user_id, current_question['id'], points)

            # Update weekly_points
            await update_weekly_points(user_id, points)

            # Update leaderboard (total points)
            await conn.execute('''
                INSERT INTO leaderboard (user_id, points) 
                VALUES ($1, $2)
                ON CONFLICT (user_id) 
                DO UPDATE SET points = leaderboard.points + EXCLUDED.points
            ''', user_id, points)

            await ctx.send(f"{emoji} {result_message} Your answer is {similarity:.2%} similar to the expected answer. You've {'earned' if correct else 'lost'} {abs(points)} points.")

            total_points = await conn.fetchval('SELECT points FROM leaderboard WHERE user_id = $1', user_id)
            weekly_points = await get_weekly_points(user_id)
            streak = await get_user_streak(user_id)
            encouragement_options = ['Keep it up!', 'Great job!', "You're doing great!", 'Keep learning!']
            encouragement = random.choice(encouragement_options)
            await ctx.send(f"Your total score is now {total_points} points. This week, you've earned {weekly_points} points. Your current streak is {streak} days. {encouragement}")

            if not correct and (user_id not in user_attempts or user_attempts[user_id] == 0):
                user_attempts[user_id] = 1
                await ctx.send("Would you like to try again? Type `!try_again` to make another attempt.")
                if current_question['hint']:
                    await ctx.send(f"Hint: {current_question['hint']}")
                else:
                    await ctx.send("Sorry, no hint is available for this question.")
            else:
                user_attempts[user_id] = 0

            # Add prompt for reporting
            await ctx.send(f"If you think this question is incorrect or needs improvement, you can report it using the `!report` command. For example: `!report {current_question['id']} The question is ambiguous and needs clarification.`")

        # Reset the skip flag for this user
        user_skips[user_id] = False

    except Exception as e:
        logging.error(f"Error in submit command: {e}")
        await ctx.send("An error occurred while processing your submission. Please try again later.")

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
    global current_question, user_skips
    user_id = ctx.author.id

    if not current_question:
        await ctx.send("There's no active question. Use `!interview` to get a question first.")
        return

    if user_id in user_skips and user_skips[user_id]:
        await ctx.send("You've already used your skip for this question. Please answer the current question or wait for the next one.")
        return

    try:
        new_question = await get_question(user_id=user_id)
        if new_question:
            current_question = new_question
            user_skips[user_id] = True
            await display_question(ctx, current_question)
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

@tasks.loop(time=time(hour=18))  # 6 PM UTC
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

@tasks.loop(time=time(hour=22))  # 10 PM UTC
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

@tasks.loop(time=time(hour=22))  # 10 PM UTC
async def post_weekly_heroes():
    if datetime.now().weekday() == 6:  # Sunday
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
                for channel_id in CHANNEL_IDS:
                    channel = bot.get_channel(channel_id)
                    if channel:
                        await channel.send(f"🦸 Weekly Heroes 🦸\n{table}")
            else:
                for channel_id in CHANNEL_IDS:
                    channel = bot.get_channel(channel_id)
                    if channel:
                        await channel.send("No submissions this week. Let's see some heroes next week!")
        except Exception as e:
            logging.error(f"Error in post_weekly_heroes task: {e}")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(start_healthcheck())
    bot.run(os.getenv('DISCORD_TOKEN'))
