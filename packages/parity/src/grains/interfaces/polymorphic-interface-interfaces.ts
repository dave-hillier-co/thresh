// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/UnitTestGrainInterfaces.cs @ v10.1.0 (MIT).
//
// Upstream `CommonMethod()` is redeclared on IA/IB/IC and given three distinct
// bodies on the implementing grain via C#'s explicit interface implementation
// (`Task<string> IA.CommonMethod()` vs `Task<string> IB.CommonMethod()` vs
// `Task<string> IC.CommonMethod()`) — the same method name resolves to a
// different body depending on which interface a caller's static type is. This
// framework dispatches by method name on the grain instance with no notion of
// "the interface the caller views the reference through", so there is no way
// to give one grain three different bodies for one method name. `commonMethod`
// is therefore omitted from every interface below; the one upstream test that
// exercises it (`Polymorphic_InheritedMethodAmbiguity`) is excluded.
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

export interface IA extends GrainKey<bigint> {
  a1Method(): Promise<string>;
  a2Method(): Promise<string>;
  a3Method(): Promise<string>;
}

export const IA = defineGrainInterface<IA>("UnitTests.GrainInterfaces.IA");

export interface IB extends GrainKey<bigint> {
  b1Method(): Promise<string>;
  b2Method(): Promise<string>;
  b3Method(): Promise<string>;
}

export const IB = defineGrainInterface<IB>("UnitTests.GrainInterfaces.IB");

export interface IC extends IA, IB {
  c1Method(): Promise<string>;
  c2Method(): Promise<string>;
  c3Method(): Promise<string>;
}

export const IC = defineGrainInterface<IC>("UnitTests.GrainInterfaces.IC");

export interface ID extends IC {
  d1Method(): Promise<string>;
  d2Method(): Promise<string>;
  d3Method(): Promise<string>;
}

export const ID = defineGrainInterface<ID>("UnitTests.GrainInterfaces.ID");

export interface IE extends GrainKey<bigint> {
  e1Method(): Promise<string>;
  e2Method(): Promise<string>;
  e3Method(): Promise<string>;
}

export const IE = defineGrainInterface<IE>("UnitTests.GrainInterfaces.IE");

export interface IF extends ID, IE {
  f1Method(): Promise<string>;
  f2Method(): Promise<string>;
  f3Method(): Promise<string>;
}

export const IF = defineGrainInterface<IF>("UnitTests.GrainInterfaces.IF");

export interface IH extends GrainKey<bigint> {
  h1Method(): Promise<string>;
  h2Method(): Promise<string>;
  h3Method(): Promise<string>;
}

export const IH = defineGrainInterface<IH>("UnitTests.GrainInterfaces.IH");

export interface IServiceType extends IF {
  serviceTypeMethod1(): Promise<string>;
  serviceTypeMethod2(): Promise<string>;
  serviceTypeMethod3(): Promise<string>;
}

export const IServiceType = defineGrainInterface<IServiceType>(
  "UnitTests.GrainInterfaces.IServiceType",
);

export interface IDerivedServiceType extends IServiceType, IH {
  derivedServiceTypeMethod1(): Promise<string>;
}

export const IDerivedServiceType = defineGrainInterface<IDerivedServiceType>(
  "UnitTests.GrainInterfaces.IDerivedServiceType",
);

/** Upstream `IPolymorphicTestGrain : IF` — a plain re-export, no added members. */
export type IPolymorphicTestGrain = IF;

export const IPolymorphicTestGrain = defineGrainInterface<IPolymorphicTestGrain>(
  "UnitTests.GrainInterfaces.IPolymorphicTestGrain",
);
