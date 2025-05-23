from aioprometheus import Counter, Gauge

PROM_EVENT_COUNTER = Counter("events", "Number of events.")
# PROM_PRICE_SUBMISSION = Counter("price_submissions", "Price Submissions.")
# PROM_API_PRICE = Counter("api_price", "API Price Actions.")
# PROM_CURRENT_BLOCK = Gauge("current_block_number", "current_block_number.")

PROM_BLOCK_NUMBER = Gauge("block_number", "Block Number")

PROM_LOG_MESSAGES_SENT = Counter(
    "total_log_messages_sent",
    "Total Messages Sent."
)