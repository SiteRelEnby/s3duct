"""Glacier/GDA thaw management (stub for future implementation)."""

# Stretch goal: initiate restore requests for all chunks,
# poll for completion, then proceed with download.
#
# Usage would be:
#   s3pipe get --bucket mybucket --name mystream --thaw --thaw-tier bulk
#
# Which would:
# 1. Check if chunks need thawing (storage class GLACIER/DEEP_ARCHIVE)
# 2. Issue restore requests for all chunks
# 3. Poll periodically, report progress
# 4. Once all thawed, begin streaming to stdout
