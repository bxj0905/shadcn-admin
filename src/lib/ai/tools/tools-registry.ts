import type { InferUITools, ToolSet } from "ai";
import { getWeatherTool } from "@/lib/ai/tools/get-weather";

export const tools = {
	"get-weather": getWeatherTool,
	// Add more tools here
} satisfies ToolSet;

export type Tools = InferUITools<typeof tools>;

export type toolId = keyof typeof tools;
