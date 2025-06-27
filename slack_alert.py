# CORRER UNA SOLA VEZ 

from prefect.blocks.notifications import SlackWebhook

# Guarda el webhook (solo se corre una vez para guardarlo en Prefect)
slack_block = SlackWebhook(url="https://hooks.slack.com/services/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")
slack_block.save("slack-alert", overwrite=True)

