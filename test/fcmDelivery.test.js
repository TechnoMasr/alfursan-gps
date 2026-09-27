const { describe, it, before, after, beforeEach } = require("node:test");
const assert = require("node:assert/strict");

const firebasePath = require.resolve("../firebase");
let mockSendImpl = async () => "projects/test/messages/ok";

function installFirebaseMock() {
  require.cache[firebasePath] = {
    id: firebasePath,
    filename: firebasePath,
    loaded: true,
    exports: {
      messaging() {
        return {
          send: (...args) => mockSendImpl(...args),
        };
      },
    },
  };
}

function clearFcmServiceCache() {
  const fcmPath = require.resolve("../fcm.service");
  delete require.cache[fcmPath];
}

installFirebaseMock();
clearFcmServiceCache();

const {
  buildFcmUserIdQuery,
  sendPushNotification,
  maybeLogFcmFailure,
  isFcmFailureLogEnabled,
} = require("../fcm.service");

/** Simulate Mongo type-sensitive equality against $in candidates. */
function findTokensByQuery(tokens, query) {
  const candidates = query?.user_id?.$in || [];
  return tokens.filter((t) => candidates.some((c) => Object.is(c, t.user_id)));
}

describe("FCM user_id candidate lookup", () => {
  it("A: numeric device_owner_id finds string fcm_tokens.user_id", () => {
    const query = buildFcmUserIdQuery(123);
    assert.ok(query);
    const found = findTokensByQuery([{ user_id: "123", fcm_token: "tokA" }], query);
    assert.equal(found.length, 1);
    assert.equal(found[0].fcm_token, "tokA");
  });

  it("B: string device_owner_id finds numeric fcm_tokens.user_id", () => {
    const query = buildFcmUserIdQuery("123");
    assert.ok(query);
    const found = findTokensByQuery([{ user_id: 123, fcm_token: "tokB" }], query);
    assert.equal(found.length, 1);
    assert.equal(found[0].fcm_token, "tokB");
  });

  it("null/undefined owner yields no query", () => {
    assert.equal(buildFcmUserIdQuery(null), null);
    assert.equal(buildFcmUserIdQuery(undefined), null);
    assert.equal(buildFcmUserIdQuery(""), null);
  });

  it("exact-type-only lookup would miss (documents the bug)", () => {
    const deviceOwnerId = 123;
    const tokens = [{ user_id: "123", fcm_token: "tok" }];
    const buggy = tokens.filter((t) => t.user_id === deviceOwnerId);
    assert.equal(buggy.length, 0);
    const fixed = findTokensByQuery(tokens, buildFcmUserIdQuery(deviceOwnerId));
    assert.equal(fixed.length, 1);
  });
});

describe("sendPushNotification error propagation", () => {
  beforeEach(() => {
    mockSendImpl = async () => "projects/test/messages/ok";
  });

  it("C: valid token → Firebase result propagates as success", async () => {
    const res = await sendPushNotification({
      token: "valid-token",
      title: "Hello",
      body: "World",
      data: { type: "test" },
    });
    assert.equal(res, "projects/test/messages/ok");
  });

  it("D: Firebase rejects → sendPushNotification rejects", async () => {
    const err = new Error("token gone");
    err.code = "messaging/registration-token-not-registered";
    mockSendImpl = async () => {
      throw err;
    };

    await assert.rejects(
      () =>
        sendPushNotification({
          token: "dead-token",
          title: "Hello",
          body: "World",
        }),
      (e) => e === err
    );
  });

  it("E: caller catches Firebase rejection and continues", async () => {
    mockSendImpl = async () => {
      throw new Error("fcm down");
    };

    let continued = false;
    let pushFailed = false;
    try {
      await sendPushNotification({
        token: "t",
        title: "t",
        body: "b",
      });
    } catch {
      pushFailed = true;
    }
    continued = true;

    assert.equal(pushFailed, true);
    assert.equal(continued, true);
  });
});

describe("optional FCM failure Mongo logging", () => {
  const mongoosePath = require.resolve("mongoose");
  let inserted = [];
  let prevFlag;

  before(() => {
    prevFlag = process.env.FCM_FAILURE_LOG_ENABLED;
  });

  after(() => {
    if (prevFlag === undefined) delete process.env.FCM_FAILURE_LOG_ENABLED;
    else process.env.FCM_FAILURE_LOG_ENABLED = prevFlag;
    delete require.cache[mongoosePath];
  });

  beforeEach(() => {
    inserted = [];
    require.cache[mongoosePath] = {
      id: mongoosePath,
      filename: mongoosePath,
      loaded: true,
      exports: {
        connection: {
          readyState: 1,
          collection() {
            return {
              async insertOne(doc) {
                inserted.push(doc);
                return { acknowledged: true };
              },
            };
          },
        },
      },
    };
  });

  it("F: failure logging disabled → no Mongo failure write", async () => {
    process.env.FCM_FAILURE_LOG_ENABLED = "false";
    assert.equal(isFcmFailureLogEnabled(), false);
    await maybeLogFcmFailure({
      imei: "1",
      user_id: 9,
      token: "abcdefghij",
      error: new Error("boom"),
      notification_type: "alarm",
    });
    assert.equal(inserted.length, 0);
  });

  it("G: failure logging enabled → one minimal failure record", async () => {
    process.env.FCM_FAILURE_LOG_ENABLED = "true";
    assert.equal(isFcmFailureLogEnabled(), true);
    const err = new Error("quota");
    err.code = "messaging/quota-exceeded";
    await maybeLogFcmFailure({
      imei: "356000",
      user_id: 42,
      token: "XXXXXXXXtail8abc",
      error: err,
      notification_type: "overspeed",
    });
    assert.equal(inserted.length, 1);
    const doc = inserted[0];
    assert.equal(doc.imei, "356000");
    assert.equal(doc.user_id, 42);
    assert.equal(doc.token_tail, "tail8abc");
    assert.equal(doc.error_code, "messaging/quota-exceeded");
    assert.equal(doc.error_message, "quota");
    assert.equal(doc.notification_type, "overspeed");
    assert.ok(doc.created_at instanceof Date);
    assert.equal(Object.prototype.hasOwnProperty.call(doc, "fcm_token"), false);
  });
});
