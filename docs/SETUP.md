# Setup

## Quick Start For Windows

If you are on Windows, the easiest path is:

1. Run [VySol.bat](../VySol.bat).
2. Let it check for supported Python and Node.js versions.
3. If something is missing, it will try to install it with `winget`.
4. It will create or reuse the backend virtual environment, install dependencies, and launch the app.

What `VySol.bat` expects:

- Windows
- `winget` available if prerequisites need to be installed
- Python 3.10 or newer
- Node.js 18 or newer

If Python and Node are already installed, the launcher will reuse them instead of reinstalling them.

## Manual Setup

If you do not want to use the batch file, you can run the app manually.

Requirements:

- Python 3.10 or newer
- Node.js 18 or newer
- npm

Backend:

```bash
cd backend
python -m venv venv
# Windows: venv\Scripts\activate
# macOS/Linux: source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Frontend:

```bash
cd frontend
npm install
npm run dev
```

By default the frontend talks to `http://localhost:8000`.

## First Run Behavior

- `settings/settings.json` is created automatically when the backend first needs a live settings file
- local worlds, graphs, vectors, and chat history are stored under `saved_worlds/`
- Gemini API keys added in the app are stored locally in `settings/settings.json`
- this public repo does not ship with live secrets, saved worlds, imported corpora, or personal runtime data
