[![](https://jitpack.io/v/justprodev/LongLivedCache.svg)](https://jitpack.io/#justprodev/LongLivedCache)
![tests](https://github.com/justprodev/LongLivedCache/actions/workflows/test.yml/badge.svg)
[![codecov](https://codecov.io/gh/justprodev/LongLivedCache/graph/badge.svg?token=MJXRVV8W92)](https://codecov.io/gh/justprodev/LongLivedCache)

The cache for a some heavily loaded but few updatable services
Cached entities can be connected by relation child->parents.
This connection guarantees that parent always be updated if child is wanted to be updated.

