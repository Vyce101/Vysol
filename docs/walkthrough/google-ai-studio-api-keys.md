# How To Get Google AI Studio API Keys

This guide is for generating Gemini API keys in Google AI Studio and using them safely in VySol.

Back to [Walkthrough](../WALKTHROUGH.md).

## Create A Key In Google AI Studio

1. Go to [Google AI Studio](https://aistudio.google.com/) and sign in.
2. Open the API key area (`Get API key` in the UI).
3. Create a new key.
4. If prompted, choose the project you want the key tied to.
5. Copy the key once it is shown.

Notes:

- keys are tied to a project context
- quotas and limits can differ by project, model, and account state
- limits can change over time, so always verify current limits in your Google dashboards

## Add The Key In VySol

1. Open VySol settings.
2. In `API Keys`, click `+`.
3. Paste the key.
4. Save your settings.

If you use multiple keys, VySol can rotate them using `Fail Over` or `Round Robin`.

## Basic Key Safety Rules

- never commit live keys to git
- never paste live keys into public chats, tickets, screenshots, or logs
- keep keys in local settings or local env files only
- rotate and replace a key immediately if you think it leaked

## Limits, Usage, And Abuse

- usage limits are enforced by Google at the project/account level and can vary by model
- hitting limits can cause throttling, request failures, or temporary blocks
- abusive or policy-violating traffic can lead to stronger enforcement, including key or account restrictions

Use keys responsibly and monitor usage in your Google tooling so you can catch spikes early.
