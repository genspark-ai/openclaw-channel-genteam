# GenTeam channel for OpenClaw

Connect a self-hosted OpenClaw gateway to **GenTeam** as an Agent runtime — GenTeam
becomes a channel in your gateway, like Slack or Discord. Inbound GenTeam messages run
through your own agent, and replies post back into GenTeam under that agent's identity.
All traffic flows through the GenTeam backend; the plugin connects to nothing else.

## Install

```bash
openclaw plugins install clawhub:@genspark/genteam
```

## Configure

In GenTeam, create an **External OpenClaw** teammate — its agent profile gives you the
`endpoint`, `channelId`, `appToken`, and `botToken`. Add a `genteam` channel to
`~/.openclaw/openclaw.json`:

```json
{
  "channels": {
    "genteam": {
      "accounts": {
        "default": {
          "endpoint": "https://www.genspark.ai",
          "channelId": "<from agent profile>",
          "appToken": "<from agent profile>",
          "botToken": "<from agent profile>",
          "attachmentRoots": ["/home/user/.openclaw/genteam-attachments"]
        }
      }
    }
  }
}
```

`attachmentRoots` is optional but recommended. The attachment upload tool only
reads files whose real path is inside one of these directories; omit or leave it
empty to disable local attachment uploads.

Then restart the gateway:

```bash
openclaw gateway restart
```

The agent shows **Online** in GenTeam once the gateway connects. Message it in a GenTeam
channel and it runs the turn on your gateway and replies in place. Your agent gets
GenTeam's actions — reading messages, posting replies, tasks, attachments — as built-in
tools, no extra setup.
