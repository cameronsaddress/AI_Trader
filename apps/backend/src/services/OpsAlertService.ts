import axios from 'axios';
import { logger } from '../utils/logger';
import type { OpsAlertPayload } from '../modules/execution/arbExecutionPipeline';

type Severity = OpsAlertPayload['severity'];

const severityRank: Record<Severity, number> = {
    INFO: 1,
    WARNING: 2,
    CRITICAL: 3,
};

function asUrls(raw: string): string[] {
    return raw
        .split(',')
        .map((entry) => entry.trim())
        .filter((entry) => /^https?:\/\//i.test(entry));
}

export class OpsAlertService {
    private readonly telegramToken: string;
    private readonly telegramChatId: string;
    private readonly webhookUrls: string[];
    private readonly enabled: boolean;
    private readonly minSeverity: Severity;
    private readonly cooldownMs: number;
    private readonly requestTimeoutMs: number;
    private readonly dedupeByScope = new Map<string, number>();

    public constructor() {
        this.telegramToken = (process.env.OPS_ALERT_TELEGRAM_BOT_TOKEN || '').trim();
        this.telegramChatId = (process.env.OPS_ALERT_TELEGRAM_CHAT_ID || '').trim();
        this.webhookUrls = asUrls(process.env.OPS_ALERT_WEBHOOK_URLS || '');
        this.cooldownMs = Math.max(1_000, Number(process.env.OPS_ALERT_COOLDOWN_MS || '30000'));
        this.requestTimeoutMs = Math.max(1_000, Number(process.env.OPS_ALERT_HTTP_TIMEOUT_MS || '6000'));
        const rawMinSeverity = (process.env.OPS_ALERT_MIN_SEVERITY || 'WARNING').trim().toUpperCase();
        this.minSeverity = rawMinSeverity === 'INFO'
            ? 'INFO'
            : rawMinSeverity === 'CRITICAL'
                ? 'CRITICAL'
                : 'WARNING';
        this.enabled = process.env.OPS_ALERTING_ENABLED === 'true'
            || Boolean(this.telegramToken && this.telegramChatId)
            || this.webhookUrls.length > 0;
    }

    public isEnabled(): boolean {
        return this.enabled;
    }

    private shouldSend(alert: OpsAlertPayload): boolean {
        if (!this.enabled) {
            return false;
        }
        if (severityRank[alert.severity] < severityRank[this.minSeverity]) {
            return false;
        }
        const key = `${alert.scope}:${alert.strategy || 'NA'}:${alert.market_key || 'NA'}:${alert.message}`;
        const now = Date.now();
        const lastSent = this.dedupeByScope.get(key) || 0;
        if (now - lastSent < this.cooldownMs) {
            return false;
        }
        this.dedupeByScope.set(key, now);
        if (this.dedupeByScope.size > 2_000) {
            const cutoff = now - (this.cooldownMs * 4);
            for (const [entryKey, ts] of this.dedupeByScope.entries()) {
                if (ts < cutoff) {
                    this.dedupeByScope.delete(entryKey);
                }
            }
        }
        return true;
    }

    private formatText(alert: OpsAlertPayload): string {
        const lines = [
            `[${alert.severity}] ${alert.scope}`,
            alert.message,
        ];
        if (alert.execution_id) {
            lines.push(`execution_id: ${alert.execution_id}`);
        }
        if (alert.strategy) {
            lines.push(`strategy: ${alert.strategy}`);
        }
        if (alert.market_key) {
            lines.push(`market_key: ${alert.market_key}`);
        }
        lines.push(`timestamp: ${new Date(alert.timestamp).toISOString()}`);
        return lines.join('\n').slice(0, 3_900);
    }

    public async notify(alert: OpsAlertPayload): Promise<void> {
        if (!this.shouldSend(alert)) {
            return;
        }
        const payload = {
            ...alert,
            iso_timestamp: new Date(alert.timestamp).toISOString(),
        };

        if (this.telegramToken && this.telegramChatId) {
            const telegramUrl = `https://api.telegram.org/bot${this.telegramToken}/sendMessage`;
            try {
                await axios.post(telegramUrl, {
                    chat_id: this.telegramChatId,
                    text: this.formatText(alert),
                    disable_web_page_preview: true,
                }, {
                    timeout: this.requestTimeoutMs,
                });
            } catch (error) {
                logger.warn(`[OpsAlert] telegram send failed: ${String(error)}`);
            }
        }

        for (const webhookUrl of this.webhookUrls) {
            try {
                await axios.post(webhookUrl, payload, {
                    timeout: this.requestTimeoutMs,
                });
            } catch (error) {
                logger.warn(`[OpsAlert] webhook send failed url=${webhookUrl}: ${String(error)}`);
            }
        }
    }
}
