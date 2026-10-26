/**
 * 出站发送账本：同一条入站消息的同一段文本只允许真正发出去一次。
 *
 * 为什么需要这个（而不是只靠 DispatchDeliverState.deliveredTexts）：
 *   `deliveredTexts` 是「按派发批次」去重的，同一条入站消息可能被派发多次——
 *   核心遇到 DispatchSessionRefreshRequiredError 会整体重试一次，插件的
 *   withDispatchRetry 也会重试，steer / queue policy 还可能让同一条消息先被
 *   活跃 run 采纳、再被新 run 处理。每次派发都会新建一份 deliveredTexts，
 *   跨派发放就拦不住，表现就是同一段回复在 QQ 里出现两遍。
 *
 * 语义（两阶段，抢占 → 确认/释放）：
 *   key = accountId : 入站 messageId : sha1(文本)
 *   - reserve：先占位（在途窗口 OUTBOUND_LEDGER_INFLIGHT_TTL_MS），并发派发互相拦截；
 *   - confirm：真正发出去之后才把占位延长到 OUTBOUND_LEDGER_TTL_MS；
 *   - release：发送失败立刻撤销占位，让重试还能再发一次。
 *   同一入站消息重发同一段文本 → 丢弃并打印日志；
 *   用户再问一次是新的 messageId → 照常发送，不会被误伤。
 */
import { createHash } from 'node:crypto';

/** 账本保留窗口。一分钟内重试、几分钟后的事件重放都要能拦住。 */
export const OUTBOUND_LEDGER_TTL_MS = 30 * 60 * 1000;
/**
 * 在途占位窗口。只管「已经 reserve、还没确认/释放」的这段时间：
 * 足够覆盖一次发送 + 紧随其后的并发重试，又不至于在异常路径上把回复锁死 30 分钟。
 */
export const OUTBOUND_LEDGER_INFLIGHT_TTL_MS = 2 * 60 * 1000;
/** 上限保护：超了以后按过期时间淘汰最旧的一批，避免长跑进程内存漂移。 */
export const OUTBOUND_LEDGER_MAX_ENTRIES = 4000;

const entries = new Map<string, number>();

function evictIfNeeded(now: number): void {
  if (entries.size <= OUTBOUND_LEDGER_MAX_ENTRIES) {
    for (const [key, expiresAt] of entries) {
      if (expiresAt <= now) entries.delete(key);
      if (entries.size <= OUTBOUND_LEDGER_MAX_ENTRIES) break;
    }
    return;
  }
  const sorted = [...entries.entries()].sort((a, b) => a[1] - b[1]);
  const dropCount = entries.size - OUTBOUND_LEDGER_MAX_ENTRIES;
  for (let i = 0; i < dropCount; i += 1) {
    const victim = sorted[i];
    if (victim) entries.delete(victim[0]);
  }
}

export function buildOutboundLedgerKey(
  accountId: string,
  inboundMessageId: string | undefined,
  text: string,
): string {
  const digest = createHash('sha1').update(text).digest('hex');
  return `${accountId}:${inboundMessageId ?? '-'}:${digest}`;
}

function isLive(expiresAt: number | undefined, now: number): boolean {
  return expiresAt !== undefined && expiresAt > now;
}

/**
 * 申请发送权（第一阶段：占位）。
 * @returns true 表示这段文本暂时归你发；false 表示已有人在发或已经发过，应当丢弃。
 */
export function reserveOutboundSend(
  accountId: string,
  inboundMessageId: string | undefined,
  text: string,
  now: number = Date.now(),
): boolean {
  if (!text) return true;
  evictIfNeeded(now);
  const key = buildOutboundLedgerKey(accountId, inboundMessageId, text);
  if (isLive(entries.get(key), now)) return false;
  entries.set(key, now + OUTBOUND_LEDGER_INFLIGHT_TTL_MS);
  return true;
}

/**
 * 确认发送成功（第二阶段）：把在途占位延长为正式保留窗口。
 */
export function confirmOutboundSend(
  accountId: string,
  inboundMessageId: string | undefined,
  text: string,
  now: number = Date.now(),
): void {
  if (!text) return;
  entries.set(buildOutboundLedgerKey(accountId, inboundMessageId, text), now + OUTBOUND_LEDGER_TTL_MS);
}

/**
 * 撤销占位（发送失败时调用）：让重试还能再发一次。
 */
export function releaseOutboundSend(
  accountId: string,
  inboundMessageId: string | undefined,
  text: string,
): void {
  if (!text) return;
  entries.delete(buildOutboundLedgerKey(accountId, inboundMessageId, text));
}

/** 测试用：清空账本。 */
export function resetOutboundLedger(): void {
  entries.clear();
}

/** 测试用：当前账本条目数。 */
export function outboundLedgerSize(): number {
  return entries.size;
}
