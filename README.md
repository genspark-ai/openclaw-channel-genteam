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

`attachmentDownloadDir` is optional. When the agent reads an inbound attachment,
`de_attachment_view` downloads the file to this directory and returns its local
path so the agent can open the contents (for example, Read an image). It
defaults to a private, unpredictably-named per-process subdirectory of the
system temp dir (created mode `0700`, with each file written `0600`); set it to
a directory your agent's file tools can read if the gateway runs the agent
workspace-rooted. The download is streamed to disk and capped at 1 GiB (override
per account with `attachmentDownloadMaxBytes`), so a large attachment never
buffers in the gateway's memory.

Then restart the gateway:

```bash
openclaw gateway restart
```

The agent shows **Online** in GenTeam once the gateway connects. Message it in a GenTeam
channel and it runs the turn on your gateway and replies in place. Your agent gets
GenTeam's actions — reading messages, posting replies, tasks, attachments — as built-in
tools, no extra setup.

## Which agent runs GenTeam turns

By default GenTeam turns run on the gateway's **default agent** (usually `main`) —
the same agent, workspace, and memory that serve your other channels, so the agent
you already trained recognizes you and its context in GenTeam too. Sessions stay
separate per GenTeam conversation, like any other channel.

To isolate GenTeam onto a dedicated agent instead (its own workspace and memory),
set an `agentId` on the account:

```json
"default": {
  "endpoint": "…",
  "channelId": "…",
  "appToken": "…",
  "botToken": "…",
  "agentId": "genteam-worker"
}
```

### Upgrading to 0.7.x

Versions before 0.7.0 keyed GenTeam sessions to an auto-provisioned per-connection
agent with a fresh, empty workspace. After upgrading, turns run on the default agent
(above), so each GenTeam conversation starts a new session once — the agent can
re-read channel history through its message tools. The old auto-provisioned agent's
session and workspace directories are left in place and unused; delete them or keep
them, either is harmless. Set `agentId` to restore the old isolation behavior.
