-- Canonical schema is embedded in tools/build_dataset.py so a build is self-contained.
-- This file is provided for client/query-service integrations.
CREATE INDEX idx_entries_word ON entries(word);
CREATE INDEX idx_entries_freq ON entries(frequency_rank, frequency);
