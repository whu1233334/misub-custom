/**
 * 订阅 Token 轮换模块
 * 处理主订阅 mytoken 的定时轮换，并通过 Telegram 推送新链接与节点统计。
 *
 * 流程：
 *   1. 读取当前设置，生成新的随机 token
 *   2. 写回 settings.mytoken（旧链接随之失效）
 *   3. 拉取所有已启用订阅的节点，统计总数 / 地区 / 协议
 *   4. 组装完整链接并通过 TG 推送
 */

import { StorageFactory } from '../../storage-adapter.js';
import { KV_KEY_SETTINGS, KV_KEY_SUBS } from '../config.js';
import { sendTgNotification } from '../notifications.js';
import { parseNodeList } from '../utils/node-parser.js';
import { ensureRegionInfo } from '../../utils/node-transformer.js';
import { buildFetchProxyUrl } from '../../utils/fetch-proxy-utils.js';

// 与 api-handler 保持一致的保留路径，避免生成会与路由冲突的 token
const RESERVED_TOKENS = new Set([
    'sub',
    'profiles',
    'api',
    'assets',
    'icons',
    'login',
    'dashboard',
    'cron',
    'callback',
    'public',
]);

/**
 * 生成一个安全的随机 token（仅用十六进制，长度足够抗枚举）
 */
function generateToken(length = 24) {
    const bytes = new Uint8Array(Math.ceil(length / 2));
    crypto.getRandomValues(bytes);
    return Array.from(bytes)
        .map((b) => b.toString(16).padStart(2, '0'))
        .join('')
        .slice(0, length);
}

/**
 * 生成一个不与保留字冲突的新 token
 */
function generateSafeToken() {
    for (let i = 0; i < 10; i += 1) {
        const token = generateToken(24);
        if (!RESERVED_TOKENS.has(token.toLowerCase())) return token;
    }
    // 极端兜底：加前缀确保唯一且非保留
    return `t${generateToken(24)}`;
}

async function loadSubscriptions(storageAdapter) {
    if (typeof storageAdapter.getAllSubscriptions === 'function') {
        const subs = await storageAdapter.getAllSubscriptions();
        if (Array.isArray(subs)) return subs;
    }
    const subs = await storageAdapter.get(KV_KEY_SUBS);
    return Array.isArray(subs) ? subs : [];
}

/**
 * 规范化协议名用于统计展示
 */
function normalizeProtocolName(proto) {
    const p = String(proto || 'unknown').toLowerCase();
    if (p === 'hy' || p === 'hy2' || p === 'hysteria') return 'hysteria2';
    return p;
}

/**
 * 若内容为 base64（订阅常见格式），解码为明文；否则原样返回。
 */
function decodeIfBase64(text) {
    const raw = String(text || '');
    // 已是明文协议列表(YAML 或 URI 行)则不解码
    if (/^(proxies:|ss:\/\/|vmess:\/\/|vless:\/\/|trojan:\/\/|hysteria)/im.test(raw)) {
        return raw;
    }
    try {
        const cleaned = raw.replace(/\s/g, '');
        let normalized = cleaned.replace(/-/g, '+').replace(/_/g, '/');
        const padding = normalized.length % 4;
        if (padding) normalized += '='.repeat(4 - padding);
        if (/^[A-Za-z0-9+/=]+$/.test(normalized) && normalized.length >= 20) {
            const binary = atob(normalized);
            const bytes = new Uint8Array(binary.length);
            for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
            return new TextDecoder('utf-8').decode(bytes);
        }
    } catch {
        /* 不是 base64，保持原样 */
    }
    return raw;
}

/**
 * 统计一段订阅文本中的节点（先解码 base64，再 parseNodeList）
 */
function statsFromText(text, plusAsSpace, totalRef, regionMap, protocolMap) {
    const decoded = decodeIfBase64(text);
    const nodes = parseNodeList(decoded, { plusAsSpace: Boolean(plusAsSpace) });
    if (!Array.isArray(nodes) || nodes.length === 0) return;
    for (const node of nodes) {
        if (!node) continue;
        totalRef.value += 1;

        const withRegion = ensureRegionInfo(node, false);
        const region = withRegion.regionZh || node.region || '其他';
        regionMap.set(region, (regionMap.get(region) || 0) + 1);

        const proto = normalizeProtocolName(node.protocol || node.type);
        protocolMap.set(proto, (protocolMap.get(proto) || 0) + 1);
    }
}

/**
 * 拉取所有启用订阅的节点并做统计
 * @returns {Promise<{total:number, regionMap:Map, protocolMap:Map}>}
 */
async function collectNodeStats(env, subscriptions, settings) {
    const regionMap = new Map();
    const protocolMap = new Map();
    const totalRef = { value: 0 };

    const userAgent = 'v2rayN/7.23';
    const skipCertVerify = settings?.builtinSkipCertVerify === true;

    // 串行拉取，避免同时打爆上游；订阅数量通常有限
    for (const sub of subscriptions) {
        if (!sub || !sub.enabled) continue;
        if (!sub.url || !/^https?:\/\//i.test(sub.url)) continue;
        try {
            let requestUrl = sub.url;
            const effectiveUa = (sub.customUserAgent && sub.customUserAgent.trim()) || userAgent;
            if (sub.fetchProxy && String(sub.fetchProxy).trim()) {
                requestUrl = buildFetchProxyUrl(sub.fetchProxy, sub.url, effectiveUa);
            }

            const init = {
                headers: { 'User-Agent': effectiveUa, Accept: '*/*' },
            };
            const cfOpts = skipCertVerify ? { cf: { insecureSkipVerify: true } } : {};
            // 手动跟随重定向：部分站点(如 GitHub Pages)在自动 follow 时 workerd 会拿到裁剪内容，
            // 而 redirect:'manual' + 手动跟随可拿到完整内容。
            let response;
            let currentUrl = requestUrl;
            for (let hop = 0; hop <= 5; hop += 1) {
                response = await fetch(new Request(currentUrl, { ...init, redirect: 'manual' }), cfOpts);
                if (response.status >= 300 && response.status < 400) {
                    const loc = response.headers.get('Location');
                    if (!loc) break;
                    currentUrl = new URL(loc, currentUrl).toString();
                    continue;
                }
                break;
            }
            if (!response.ok) {
                console.warn(`[RotateToken] ${sub.name}: HTTP ${response.status}`);
                continue;
            }

            const text = await response.text();
            statsFromText(text, sub.plusAsSpace, totalRef, regionMap, protocolMap);
        } catch (err) {
            console.warn(`[RotateToken] Failed to fetch nodes for ${sub.name}:`, err?.message);
        }
    }

    return { total: totalRef.value, regionMap, protocolMap };
}

function formatDistribution(map, limit = 12) {
    return Array.from(map.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, limit)
        .map(([name, count]) => `${name} ${count}`)
        .join(' / ');
}

/**
 * 执行一次 token 轮换并推送
 * @param {Object} env
 * @param {Object} options
 * @param {string} [options.baseUrl] - 站点根地址（如 https://xxx.pages.dev），用于拼接完整订阅链接
 * @param {Object} [options.tgOverride] - 直接指定 { BotToken, ChatID }（可选，优先于设置）
 * @returns {Promise<Object>} 执行结果
 */
export async function rotateSubscriptionToken(env, options = {}) {
    const storageType = await StorageFactory.getStorageType(env);
    const storageAdapter = StorageFactory.createAdapter(env, storageType);

    const storedSettings = (await storageAdapter.get(KV_KEY_SETTINGS)) || {};
    const settings = { ...storedSettings };

    // 1. 生成并写回新 token
    const oldToken = settings.mytoken || '';
    const newToken = generateSafeToken();
    settings.mytoken = newToken;
    await storageAdapter.put(KV_KEY_SETTINGS, settings);

    // 2. 收集节点统计
    const subscriptions = await loadSubscriptions(storageAdapter);
    const stats = await collectNodeStats(env, subscriptions, settings);

    // 3. 组装完整链接
    // 拼接站点根地址（优先级：显式 MISUB_PUBLIC_URL > CF_PAGES_URL > 传入 baseUrl）
    // 注意：cron 触发时 request.url.origin 不可靠（不是站点域名），必须在调用处传 CF_PAGES_URL。
    const base = String(options.baseUrl || env?.MISUB_PUBLIC_URL || env?.CF_PAGES_URL || '').replace(
        /\/+$/,
        ''
    );
    const fullLink = base ? `${base}/sub/${newToken}` : `/sub/${newToken}`;

    // 4. 推送 TG
    const tgSettings = options.tgOverride
        ? { ...settings, ...options.tgOverride }
        : settings;

    const now = new Date().toISOString().replace('T', ' ').slice(0, 16);
    const regionText = formatDistribution(stats.regionMap) || '无';
    const protocolText = formatDistribution(stats.protocolMap) || '无';

    const message = [
        '🔗 <b>订阅链接已更新</b>',
        `时间：${now} (UTC)`,
        '',
        `<b>新链接：</b>`,
        fullLink,
        '',
        `📦 节点总数：<b>${stats.total}</b>`,
        `🌍 地区分布：${regionText}`,
        `⚙️ 协议分布：${protocolText}`,
        '',
        '⚠️ 旧链接已失效，请及时更新客户端订阅。',
    ].join('\n');

    let pushResult = { success: false, reason: 'no-tg-config' };
    if (tgSettings.BotToken && tgSettings.ChatID) {
        pushResult = await sendTgNotification(tgSettings, message);
    }

    return {
        success: true,
        newToken,
        fullLink,
        total: stats.total,
        regions: Object.fromEntries(stats.regionMap),
        protocols: Object.fromEntries(stats.protocolMap),
        push: pushResult,
        previousToken: oldToken,
    };
}
