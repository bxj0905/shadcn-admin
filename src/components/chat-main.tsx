"use client";

import { useEffect, useRef, useState } from "react";
import type { AIUIMessage } from "@/lib/ai/messages";
import { ChatContent } from "@/components/chat-content";
import { ChatHeader } from "@/components/chat-header";
import { useAuthStore } from "@/stores/auth-store";

type ChatStatus = "idle" | "submitted" | "streaming";

export function ChatMain() {
	const [messages, setMessages] = useState<AIUIMessage[]>([]);
	const [status, setStatus] = useState<ChatStatus>("idle");
	const [totalTokens, setTotalTokens] = useState<number>(0);
	const { auth } = useAuthStore();
	const currentUser = auth.user;
	const userAvatarImage =
		currentUser?.avatarUrl ?? "/avatar-1.png";
	const streamAbortRef = useRef<AbortController | null>(null);

	const estimateTokens = (text: string) => {
		// 简单估算：平均 4 个字符约等于 1 个 token
		const length = text.trim().length;
		if (!length) return 0;
		return Math.max(1, Math.round(length / 4));
	};

	const addTokens = (delta: number) => {
		if (!delta || Number.isNaN(delta)) return;
		setTotalTokens((prev) => {
			const next = Math.max(0, prev + delta);
			try {
				window.localStorage.setItem(
					"chat_total_tokens",
					String(next),
				);
			} catch {
				// 忽略本地存储错误
			}
			return next;
		});
	};

	const stop = () => {
		// 停止当前的流式渲染
		if (streamAbortRef.current) {
			streamAbortRef.current.abort();
			streamAbortRef.current = null;
		}
		setStatus("idle");
	};

	const sendMessage = async (
		message: {
			role: "user";
			parts: { type: string; text: string }[];
		},
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		_options?: any,
	) => {
		let text = message.parts?.[0]?.text ?? "";
		if (!text.trim()) return;

		// 解析 /tool 前缀指令
		let toolQuery: string | null = null;
		if (text.trimStart().startsWith("/tool")) {
			toolQuery = text.trimStart().replace(/^\/tool\s*/i, "");
			text = toolQuery || "";
		}

		// 估算并记录本次 user 输入 token 数
		addTokens(estimateTokens(text));

		const userMessage: AIUIMessage = {
			id: `${Date.now()}-user`,
			role: "user",
			parts: message.parts,
			metadata: {
				timestamp: new Date().toISOString(),
				avatarImage: userAvatarImage,
			},
		};

		setMessages((prev) => [...prev, userMessage]);
		setStatus("submitted");

		try {
			const controller = new AbortController();
			streamAbortRef.current = controller;

			// 示例工具调用：仅当消息以 /tool 开头时，插入一个 search-database 工具调用
			if (toolQuery && toolQuery.trim()) {
				const toolCallId = `search-${Date.now()}`;
				const toolInput = { query: toolQuery };
				const toolOutput = `Result of searching the database for "${toolQuery}"`;
				const toolMessage: AIUIMessage = {
					id: `${Date.now()}-tool`,
					role: "assistant",
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					parts: [
						{
							type: "tool-search-database",
							toolCallId,
							state: "output-available",
							input: toolInput,
							output: toolOutput,
						} as any,
					],
					metadata: {
						timestamp: new Date().toISOString(),
						avatarImage: "/avatar-2.png",
					},
				};

				setMessages((prev) => [...prev, toolMessage]);
			}

			const resp = await fetch("/api/deepseek/chat-stream", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ text }),
				signal: controller.signal,
			});

			if (!resp.ok || !resp.body) {
				setStatus("idle");
				return;
			}

			const assistantId = `${Date.now()}-assistant`;
			const assistantMessage: AIUIMessage = {
				id: assistantId,
				role: "assistant",
				parts: [{ type: "text", text: "" }],
				metadata: {
					timestamp: new Date().toISOString(),
					avatarImage: "/avatar-2.png",
				},
			};

			setMessages((prev) => [...prev, assistantMessage]);
			setStatus("streaming");

			const reader = resp.body.getReader();
			const decoder = new TextDecoder("utf-8");
			let accumulatedText = "";

			while (true) {
				const { value, done } = await reader.read();
				if (done) break;
				accumulatedText += decoder.decode(value, { stream: true });

				const currentText = accumulatedText;
				setMessages((prev) =>
					prev.map((m) =>
						m.id === assistantId
							? {
									...m,
									parts: [
										{ type: "text", text: currentText },
									],
							  }
							: m,
						),
					);
			}

			setStatus("idle");
		} catch (error) {
			// 可以在这里加日志或 UI 提示
			if ((error as any)?.name !== "AbortError") {
				setStatus("idle");
			}
		} finally {
			streamAbortRef.current = null;
		}
	};

	useEffect(() => {
		// 初始化从 localStorage 读取历史 token 使用量
		try {
			const stored = window.localStorage.getItem("chat_total_tokens");
			if (stored) {
				const parsed = Number(stored);
				if (!Number.isNaN(parsed)) {
					setTotalTokens(parsed);
				}
			}
		} catch {
			// ignore
		}

		return () => {
			// 组件卸载时中止流
			if (streamAbortRef.current) {
				streamAbortRef.current.abort();
				streamAbortRef.current = null;
			}
		};
	}, []);

	return (
		<div className="flex-1 flex flex-col min-h-0">
			{/* ChatHeader 只需要能清空消息，useState setter 与 useChat 的 setMessages 签名兼容 */}
			<ChatHeader setMessages={setMessages as any} />
			<ChatContent
				messages={messages as any}
				sendMessage={sendMessage as any}
				status={status as any}
				stop={stop as any}
				tokenUsage={totalTokens}
			/>
		</div>
	);
}
