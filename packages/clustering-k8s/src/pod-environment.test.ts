import { describe, expect, it } from "vitest";
import { readPodEnvironment, siloAddressFromPodEnv } from "@tsva/clustering-k8s/pod-environment";

const env = {
  POD_NAME: "silo-2",
  POD_UID: "abcd-1234",
  POD_IP: "10.1.2.3",
  POD_NAMESPACE: "actors",
};

describe("pod-environment", () => {
  it("reads the downward-API variables", () => {
    expect(readPodEnvironment(env)).toEqual({
      podName: "silo-2",
      podUid: "abcd-1234",
      podIp: "10.1.2.3",
      namespace: "actors",
    });
  });

  it("builds a SiloAddress from the pod identity and silo port", () => {
    const address = siloAddressFromPodEnv(11111, env);
    expect(address.podName).toBe("silo-2");
    expect(address.podUid).toBe("abcd-1234");
    expect(address.endpoint).toBe("10.1.2.3:11111");
    expect(address.ringKey).toBe("silo-2");
  });

  it("throws a clear error when a required variable is missing", () => {
    expect(() => readPodEnvironment({ ...env, POD_UID: undefined })).toThrow(/POD_UID/);
  });
});
