// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/Filtering/StreamFilteringTestsBase.cs @ v10.1.0 (MIT).
//
// This is an abstract base class: `IgnoreBadFilter`, `OnlyEvenItems` and
// `MultipleSubscriptionsDifferentFilterData` are plain `public virtual`
// methods with no `[Fact]`/`[Theory]` attribute of their own — only a
// concrete subclass that overrides them and adds the attribute actually runs
// as a test. Within this assignment's scope (`test/Orleans.Streaming.Tests`)
// there is no such subclass; `git grep -l StreamFilteringTestsBase v10.1.0 --
// test src` finds only `AdoNetStreamFilteringTests`, `AQStreamFilteringTests`
// and `RedisStreamFilteringTests`, all in `Orleans.AdoNet.Tests` /
// `Orleans.Azure.Tests` / `Orleans.Redis.Tests` — other test projects outside
// this assignment. So this file contributes zero upstream `[Fact]`/`[Theory]`
// methods to account for; there is nothing to port, gap, or exclude.
import { it } from "vitest";

it.skip("(StreamFilteringTestsBase declares no [Fact]/[Theory] methods of its own; its only concrete subclasses are in other, out-of-scope test projects — nothing to port)", () =>
  undefined);
