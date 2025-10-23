name: Telegram Channel Monitor

on:
  schedule:
    # Запуск каждый день в 9:00 UTC (12:00 МСК, 11:00 по Киеву/Минску)
    - cron: '0 9 * * *'
  
  # Возможность запустить вручную
  workflow_dispatch:

# Добавляем права на запись
permissions:
  contents: write

jobs:
  monitor:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout repository
      uses: actions/checkout@v3
      with:
        token: ${{ secrets.GITHUB_TOKEN }}
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Cache dependencies
      uses: actions/cache@v3
      with:
        path: ~/.cache/pip
        key: ${{ runner.os }}-pip-${{ hashFiles('requirements.txt') }}
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
    
    - name: Run monitor bot
      env:
        # Секретные данные
        TELEGRAM_API_ID: ${{ secrets.TELEGRAM_API_ID }}
        TELEGRAM_API_HASH: ${{ secrets.TELEGRAM_API_HASH }}
        SESSION_STRING: ${{ secrets.SESSION_STRING }}
        BOT_TOKEN: ${{ secrets.BOT_TOKEN }}
        YOUR_USER_ID: ${{ secrets.YOUR_USER_ID }}
        # Обычные переменные (видимые)
        CHANNELS: ${{ vars.CHANNELS }}
        KEYWORDS: ${{ vars.KEYWORDS }}
        EXCLUDE_KEYWORDS: ${{ vars.EXCLUDE_KEYWORDS }}
        PATTERNS: ${{ vars.PATTERNS }}
        SEARCH_DEPTH: ${{ vars.SEARCH_DEPTH || '100' }}
        TIME_RANGE_HOURS: ${{ vars.TIME_RANGE_HOURS || '24' }}
      run: |
        python telegram_monitor.py
    
    - name: Commit processed messages
      run: |
        git config --local user.email "github-actions[bot]@users.noreply.github.com"
        git config --local user.name "github-actions[bot]"
        git add processed_messages.json
        git diff --quiet && git diff --staged --quiet || git commit -m "Update processed messages [skip ci]"
        git push
