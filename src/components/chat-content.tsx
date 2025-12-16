"use client";

import type { useChat } from "@ai-sdk/react";
import { PlusIcon, ThumbsUp } from "lucide-react";
import type { ComponentPropsWithoutRef } from "react";
import { useEffect, useMemo } from "react";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { agentsList } from "@/lib/ai/agents/agents-registry";
import { uiMessageToText } from "@/lib/ai/ai-utils";
import type { AIUIMessage } from "@/lib/ai/messages";
import {
	ChatInput,
	ChatInputEditor,
	ChatInputGroupAddon,
	ChatInputGroupButton,
	ChatInputGroupText,
	ChatInputMention,
	ChatInputSubmitButton,
	createMentionConfig,
	useChatInput,
} from "@/components/ui/chat-input";
import {
	ChatMessage,
	ChatMessageAction,
	ChatMessageActionCopy,
	ChatMessageActions,
	ChatMessageAuthor,
	ChatMessageAvatar,
	ChatMessageAvatarFallback,
	ChatMessageAvatarImage,
	ChatMessageContainer,
	ChatMessageContent,
	ChatMessageHeader,
	ChatMessageMarkdown,
	ChatMessageTimestamp,
} from "@/components/ui/chat-message";
import {
	ChatMessageArea,
	ChatMessageAreaContent,
	ChatMessageAreaScrollButton,
} from "@/components/ui/chat-message-area";
import { Separator } from "@/components/ui/separator";
import {
	ChatSuggestion,
	ChatSuggestions,
	ChatSuggestionsContent,
	ChatSuggestionsHeader,
	ChatSuggestionsTitle,
} from "@/components/ui/chat-suggestions";
import { Reasoning } from "@/components/ui/reasoning";
import {
	ToolInvocation,
	ToolInvocationContentCollapsible,
	ToolInvocationHeader,
	ToolInvocationName,
	ToolInvocationRawData,
} from "@/components/ui/tool-invocation";
import { useStickToBottomContext } from "use-stick-to-bottom";

export const DEFAULT_CHAT_SUGGESTIONS = [
	"Hello! Can you help me with a coding question?",
	"Tell me about your capabilities and what you can do",
	"I need help organizing my project management workflow",
	"Can you explain a complex topic in simple terms?",
];

function NoChatMessages({
	onSuggestionClick,
}: {
	onSuggestionClick: (suggestion: string) => void;
}) {
	return (
		<div className="flex flex-col gap-2 p-2 justify-end items-center h-full">
			<ChatSuggestions>
				<ChatSuggestionsHeader>
					<ChatSuggestionsTitle>
						Try these prompts:
					</ChatSuggestionsTitle>
				</ChatSuggestionsHeader>
				<ChatSuggestionsContent>
					{DEFAULT_CHAT_SUGGESTIONS.map((suggestion) => (
						<ChatSuggestion
							key={suggestion}
							onClick={() => onSuggestionClick(suggestion)}
						>
							{suggestion}
						</ChatSuggestion>
					))}
				</ChatSuggestionsContent>
			</ChatSuggestions>
		</div>
	);
}

type ReturnOfUseChat = ReturnType<typeof useChat<AIUIMessage>>;

function AutoScrollOnNewMessage({ marker }: { marker: string }) {
	const { isAtBottom, scrollToBottom } = useStickToBottomContext();

	useEffect(() => {
		if (isAtBottom) {
			scrollToBottom();
		}
	}, [marker, isAtBottom, scrollToBottom]);

	return null;
}

export function ChatContent({
	className,
	messages,
	sendMessage,
	status,
	stop,
	...props
}: ComponentPropsWithoutRef<"div"> & {
	messages: ReturnOfUseChat["messages"];
	sendMessage: ReturnOfUseChat["sendMessage"];
	status: ReturnOfUseChat["status"];
	stop: ReturnOfUseChat["stop"];
	tokenUsage?: number;
}) {
	const isLoading = status === "streaming" || status === "submitted";

	// Use the new hook with custom onSubmit
	const { value, onChange, handleSubmit, mentionConfigs } = useChatInput({
		mentions: {
			agents: createMentionConfig({
				type: "agents",
				trigger: "@",
				items: agentsList,
			}),
		},
		onSubmit: (parsedValue) => {
			sendMessage(
				{
					role: "user",
					parts: [{ type: "text", text: parsedValue.content }],
				},
				{
					body: {
						mentions: parsedValue.agents,
					},
				},
			);
		},
	});

	const scrollMarker = useMemo(() => {
		if (!messages.length) return "";
		const last = messages[messages.length - 1];
		const textParts =
			last.parts
				?.filter((p) => (p as { type?: string }).type === "text")
				.map((p) => (p as { text?: string }).text ?? "")
				.join("\n") ?? "";
		return `${messages.length}-${textParts.length}`;
	}, [messages]);

	const usagePercent = useMemo(() => {
		// 如果上层传入了 tokenUsage，则直接按 tokenUsage 显示；
		// 否则回退到基于当前输入长度的简单估算。
		if (typeof props.tokenUsage === "number") {
			return props.tokenUsage;
		}
		const maxChars = 1000;
		const text = typeof value === "string" ? value : "";
		const length = text.length;
		return Math.min(100, Math.round((length / maxChars) * 100));
	}, [props.tokenUsage, value]);

	return (
		<div className="flex-1 flex flex-col min-h-0" {...props}>
			<ChatMessageArea>
				<ChatMessageAreaContent className="pt-6">
					{messages.length === 0 ? (
						<NoChatMessages
							onSuggestionClick={(suggestion) => {
								sendMessage({
									role: "user",
									parts: [{ type: "text", text: suggestion }],
								});
							}}
						/>
					) : (
						messages.map((message) => {
							const userName =
								message.role === "user"
									? "You"
									: (message.metadata?.agent?.name ??
										"Assistant");

							return (
								<ChatMessage key={message.id}>
									<ChatMessageActions>
										<ChatMessageActionCopy
											onClick={() => {
												navigator.clipboard.writeText(
													uiMessageToText(message),
												);
											}}
										/>
										<ChatMessageAction label="Like">
											<ThumbsUp className="size-4" />
										</ChatMessageAction>
									</ChatMessageActions>
									<ChatMessageAvatar>
										{(() => {
											const avatarSrc =
												message.metadata?.avatarImage ??
												(message.role === "user"
													? "/avatar-1.png"
													: message.metadata?.agent?.image ??
														"/avatar-2.png");

											return (
												<>
													<ChatMessageAvatarImage
														src={avatarSrc}
														alt={userName}
													/>
													<ChatMessageAvatarFallback>
														{userName.charAt(0).toUpperCase()}
													</ChatMessageAvatarFallback>
												</>
											);
										})()}
									</ChatMessageAvatar>

									<ChatMessageContainer>
										<ChatMessageHeader>
											<ChatMessageAuthor>
												{userName}
											</ChatMessageAuthor>
											<ChatMessageTimestamp
												createdAt={new Date()}
											/>
										</ChatMessageHeader>

										<ChatMessageContent>
											{message.parts.map(
												(part, index) => {
													if (part.type === "text") {
														return (
															<ChatMessageMarkdown
																key={`${message.id}-text-${index}`}
																content={
																	part.text
																}
															/>
														);
													}

													if (
														part.type ===
														"reasoning"
													) {
														return (
															<Reasoning
																key={`reasoning-${message.id}-${index}`}
																content={
																	part.text
																}
																isLastPart={
																	index ===
																	message
																		.parts
																		.length -
																		1
																}
															/>
														);
													}

													if (
														part.type.startsWith(
															"tool-",
														)
													) {
														if (
															!(
																"toolCallId" in
																part
															) ||
															!("state" in part)
														) {
															return null;
														}

														const toolPart =
															part as {
																type: string;
																toolCallId: string;
																state:
																	| "input-streaming"
																	| "input-available"
																	| "output-available"
																	| "output-error";
																input?: unknown;
																output?: unknown;
																errorText?: string;
															};

														const hasInput =
															toolPart.input !=
																null &&
															toolPart.input !==
																undefined;
														const hasOutput =
															toolPart.output !=
																null &&
															toolPart.output !==
																undefined;

														const toolName =
															toolPart.type.slice(
																5,
															);
														return (
															<ToolInvocation
																key={
																	toolPart.toolCallId
																}
																className="w-full"
															>
																<ToolInvocationHeader>
																	<ToolInvocationName
																		name={
																			toolName
																		}
																		type={
																			toolPart.state
																		}
																		isError={
																			toolPart.state ===
																			"output-error"
																		}
																	/>
																</ToolInvocationHeader>
																{(hasInput ||
																	hasOutput ||
																	toolPart.errorText) && (
																	<ToolInvocationContentCollapsible>
																		{hasInput && (
																			<ToolInvocationRawData
																				data={
																					toolPart.input
																				}
																				title="Arguments"
																			/>
																		)}
																		{toolPart.errorText && (
																			<ToolInvocationRawData
																				data={{
																					error: toolPart.errorText,
																				}}
																				title="Error"
																			/>
																		)}
																		{hasOutput && (
																			<ToolInvocationRawData
																				data={
																					toolPart.output
																				}
																				title="Result"
																			/>
																		)}
																	</ToolInvocationContentCollapsible>
																)}
															</ToolInvocation>
														);
													}
													return null;
												},
											)}
										</ChatMessageContent>
									</ChatMessageContainer>
								</ChatMessage>
							);
						})
					)}
				</ChatMessageAreaContent>
				<ChatMessageAreaScrollButton alignment="center" />
				<AutoScrollOnNewMessage marker={scrollMarker} />
			</ChatMessageArea>
			<div className="px-2 py-4 max-w-2xl mx-auto w-full">
				<ChatInput
					onSubmit={handleSubmit}
					isStreaming={isLoading}
					onStop={stop}
				>
					<ChatInputMention
						type={mentionConfigs.agents.type}
						trigger={mentionConfigs.agents.trigger}
						items={mentionConfigs.agents.items}
					>
						{(item) => (
							<>
								<Avatar className="h-6 w-6">
									<AvatarImage
										//src={item.image ?? "/placeholder.jpg"}
										alt={item.name}
									/>
									<AvatarFallback>
										{item.name[0].toUpperCase()}
									</AvatarFallback>
								</Avatar>

								<span
									className="text-sm font-medium truncate max-w-[120px]"
									title={item.name}
								>
									{item.name}
								</span>
								<Badge variant="outline" className="ml-auto">
									Agent
								</Badge>
							</>
						)}
					</ChatInputMention>
					<ChatInputEditor
						value={value}
						onChange={onChange}
						placeholder="Type @ for agents"
					/>
					<ChatInputGroupAddon align="block-end">
						<ChatInputGroupButton
							variant="outline"
							className="rounded-full"
							size="icon-sm"
						>
							<PlusIcon />
						</ChatInputGroupButton>
						<ChatInputGroupText className="ml-auto">
							{usagePercent}% used
						</ChatInputGroupText>
						<Separator orientation="vertical" className="h-6" />
						<ChatInputSubmitButton />
					</ChatInputGroupAddon>
				</ChatInput>
			</div>
		</div>
	);
}
