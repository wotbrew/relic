# Extension

Currently it is not recommended implementing in user programs new aggregation functions or data flow nodes.

This is because the SPI for these is under consideration and not final. There are no protocols or mechanism for extension
other than internals which I'd rather remained implementation detail for now.

If you want relic to do something new, raise a PR for discussion and it can be implemented in relic.