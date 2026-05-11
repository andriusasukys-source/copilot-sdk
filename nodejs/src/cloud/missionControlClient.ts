/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import type {
    CloudSessionEvent,
    CloudSessionFailureReason,
    MissionControlCommandType,
    MissionControlTask,
} from "../types.js";
import { stripTrailingSlash } from "../url.js";

export const CLOUD_SANDBOX_AGENT_SLUG = "copilot-developer-sandbox";

const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
const DEFAULT_CREATE_CLOUD_TASK_TIMEOUT_MS = 10 * 60 * 1000;

export interface MissionControlClientOptions {
    baseUrl: string;
    authToken?: string;
    integrationId?: string;
    frontendBaseUrl: string;
    requestTimeoutMs?: number;
    createCloudTaskTimeoutMs?: number;
}

export interface CreateCloudTaskRepository {
    owner: string;
    name: string;
}

export interface CreateCloudTaskParams {
    owner?: string;
    repository?: CreateCloudTaskRepository;
}

export class CloudSessionError extends Error {
    constructor(
        message: string,
        public readonly reason: CloudSessionFailureReason,
        public readonly status?: number
    ) {
        super(message);
        this.name = "CloudSessionError";
    }
}

export class MissionControlClient {
    private readonly baseUrl: string;
    private readonly authToken?: string;
    private readonly integrationId: string;
    private readonly frontendBaseUrl: string;
    private readonly requestTimeoutMs: number;
    private readonly createCloudTaskTimeoutMs: number;

    constructor(options: MissionControlClientOptions) {
        this.baseUrl = stripTrailingSlash(options.baseUrl);
        this.authToken = options.authToken?.trim() || undefined;
        this.integrationId = options.integrationId ?? "copilot-cli";
        this.frontendBaseUrl = stripTrailingSlash(options.frontendBaseUrl);
        this.requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
        this.createCloudTaskTimeoutMs =
            options.createCloudTaskTimeoutMs ?? DEFAULT_CREATE_CLOUD_TASK_TIMEOUT_MS;
    }

    async createCloudTask(params: CreateCloudTaskParams = {}): Promise<MissionControlTask> {
        const body: Record<string, unknown> = {};
        if (params.owner) {
            body.owner = params.owner;
        }
        if (params.repository) {
            body.repositories = [params.repository];
        }

        return this.requestJson<MissionControlTask>(
            `${this.baseUrl}/tasks`,
            {
                method: "POST",
                headers: this.headers({ "X-Copilot-Agent-Slug": CLOUD_SANDBOX_AGENT_SLUG }),
                body: JSON.stringify(body),
            },
            this.createCloudTaskTimeoutMs
        );
    }

    async listTaskEvents(taskId: string): Promise<CloudSessionEvent[]> {
        const data = await this.requestJson<{ events?: unknown[] }>(
            `${this.baseUrl}/tasks/${encodeURIComponent(taskId)}/events`,
            {
                method: "GET",
                headers: this.headers(),
            },
            this.requestTimeoutMs
        );

        if (!Array.isArray(data.events)) {
            throw new CloudSessionError(
                `Unexpected Mission Control events response for task ${taskId}`,
                "server"
            );
        }

        return data.events.map((event, index) => parseCloudSessionEvent(event, taskId, index));
    }

    async steerTask(
        taskId: string,
        request: { type: MissionControlCommandType; content?: string }
    ): Promise<void> {
        await this.requestOk(
            `${this.baseUrl}/tasks/${encodeURIComponent(taskId)}/steer`,
            {
                method: "POST",
                headers: this.headers(),
                body: JSON.stringify(request),
            },
            this.requestTimeoutMs
        );
    }

    async getTask(taskId: string): Promise<MissionControlTask | undefined> {
        try {
            return await this.requestJson<MissionControlTask>(
                `${this.baseUrl}/tasks/${encodeURIComponent(taskId)}`,
                {
                    method: "GET",
                    headers: this.headers(),
                },
                this.requestTimeoutMs
            );
        } catch (error) {
            if (error instanceof CloudSessionError && error.status === 404) {
                return undefined;
            }
            throw error;
        }
    }

    getFrontendUrl(taskId: string): string {
        return `${this.frontendBaseUrl}/copilot/tasks/${encodeURIComponent(taskId)}`;
    }

    private headers(extraHeaders?: Record<string, string>): Record<string, string> {
        const headers: Record<string, string> = {
            "Content-Type": "application/json",
            "Copilot-Integration-Id": this.integrationId,
            ...extraHeaders,
        };
        if (this.authToken) {
            headers.Authorization = `Bearer ${this.authToken}`;
        }
        return headers;
    }

    private async requestJson<T>(url: string, init: RequestInit, timeoutMs: number): Promise<T> {
        const response = await this.requestOk(url, init, timeoutMs);
        const text = await response.text();
        if (!text) {
            return undefined as T;
        }
        try {
            return JSON.parse(text) as T;
        } catch (error) {
            throw new CloudSessionError(
                `Mission Control returned invalid JSON: ${error instanceof Error ? error.message : String(error)}`,
                "server"
            );
        }
    }

    private async requestOk(url: string, init: RequestInit, timeoutMs: number): Promise<Response> {
        try {
            const response = await fetch(url, {
                ...init,
                signal: AbortSignal.timeout(timeoutMs),
            });

            if (!response.ok) {
                const text = await response.text().catch(() => "");
                throw new CloudSessionError(
                    extractMissionControlMessage(text) ||
                        `Mission Control request failed with HTTP ${response.status}`,
                    reasonForStatus(response.status),
                    response.status
                );
            }

            return response;
        } catch (error) {
            if (error instanceof CloudSessionError) {
                throw error;
            }
            if (isAbortError(error)) {
                throw new CloudSessionError("Mission Control request timed out", "timeout");
            }
            throw new CloudSessionError(
                `Mission Control request failed: ${error instanceof Error ? error.message : String(error)}`,
                "network"
            );
        }
    }
}

function reasonForStatus(status: number): CloudSessionFailureReason {
    if (status === 403) return "policy_blocked";
    if (status === 400 || status === 422) return "validation";
    return "server";
}

function extractMissionControlMessage(text: string): string | undefined {
    if (!text) return undefined;
    try {
        const parsed = JSON.parse(text) as { message?: unknown };
        if (typeof parsed.message === "string" && parsed.message.length > 0) {
            return parsed.message;
        }
    } catch {
        // Non-JSON responses are surfaced as-is below.
    }
    return text;
}

function isAbortError(error: unknown): boolean {
    return error instanceof Error && (error.name === "AbortError" || error.name === "TimeoutError");
}

function parseCloudSessionEvent(value: unknown, taskId: string, index: number): CloudSessionEvent {
    const label = `Mission Control event ${index} for task ${taskId}`;
    if (!isRecord(value)) {
        throw invalidEventShape(label, "expected an object");
    }

    if (typeof value.id !== "string") {
        throw invalidEventShape(label, "expected string id");
    }
    if (typeof value.timestamp !== "string") {
        throw invalidEventShape(label, "expected string timestamp");
    }
    if (typeof value.type !== "string") {
        throw invalidEventShape(label, "expected string type");
    }
    if (value.parentId !== null && typeof value.parentId !== "string") {
        throw invalidEventShape(label, "expected parentId to be a string or null");
    }
    if (value.ephemeral !== undefined && typeof value.ephemeral !== "boolean") {
        throw invalidEventShape(label, "expected ephemeral to be a boolean");
    }
    if (value.agentId !== undefined && typeof value.agentId !== "string") {
        throw invalidEventShape(label, "expected agentId to be a string");
    }

    validateKnownEventShape(value, label);
    return value as unknown as CloudSessionEvent;
}

function validateKnownEventShape(event: Record<string, unknown>, label: string): void {
    if (event.type === "session.requested") {
        if (event.data !== undefined && !isRecord(event.data)) {
            throw invalidEventShape(label, "expected session.requested data to be an object");
        }
        return;
    }

    const data =
        typeof event.type === "string" && event.type.startsWith("session.")
            ? requireDataObject(event, label)
            : event.data;

    if (event.type === "session.remote_steerable_changed") {
        if (!isRecord(data) || typeof data.remoteSteerable !== "boolean") {
            throw invalidEventShape(
                label,
                "expected session.remote_steerable_changed data.remoteSteerable to be a boolean"
            );
        }
    } else if (event.type === "session.error") {
        if (!isRecord(data) || typeof data.message !== "string") {
            throw invalidEventShape(label, "expected session.error data.message to be a string");
        }
    } else if (event.type === "assistant.message") {
        const messageData = requireDataObject(event, label);
        if (typeof messageData.content !== "string" || typeof messageData.messageId !== "string") {
            throw invalidEventShape(
                label,
                "expected assistant.message data.content and data.messageId to be strings"
            );
        }
    }
}

function requireDataObject(event: Record<string, unknown>, label: string): Record<string, unknown> {
    if (!isRecord(event.data)) {
        throw invalidEventShape(label, `expected ${String(event.type)} data to be an object`);
    }
    return event.data;
}

function invalidEventShape(label: string, detail: string): CloudSessionError {
    return new CloudSessionError(`Unexpected ${label}: ${detail}`, "server");
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
