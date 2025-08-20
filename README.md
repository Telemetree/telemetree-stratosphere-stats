## Setting up

### Environment
1. Install uv if you don't have it
2. Use `uv venv` to create virtual environment
3. `source .venv/activate/bin` to activate it
4. `uv sync` to install dependencies

### Telegram
1. Get your `API_ID` and `API_HASH` from https://my.telegram.org/apps 
```
# TELEGRAM
TELEGRAM_API_ID=
TELEGRAM_API_HASH=""
```
2. You'll need a Telegram account to extract stats. You can use your own. 
    a. You'll be asked to log in if you don't have a session. It'll ask for the phone number, verification code and 2FA is you have one.
    b. If you have saved session, it will log in automatically.
3. You can change session name at `src/telegram/telegram_constants.py`. This will allow you to manage multiple sessions.

### Notion
1. Put your Notion API key into your .env file.
```
NOTION_API_KEY=""
```
2. If you set up a different databases, please, update constants at `src/notion/notion_constants.py`

## Running
1. After you're done with setup, you can run the code with `uv run main.py`