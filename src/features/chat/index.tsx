import { ChatMain } from "@/components/chat-main";

// /chat 页面作为 AuthenticatedLayout 的子路由内容渲染，
// 这里模仿 AppLayout 的结构：外层固定视口高度，
// 内层卡片 overflow-y-auto，这样只在卡片内部滚动。
export default function Page() {
	return (
		<div className="h-full flex w-full flex-1 flex-col">
			<ChatMain />
		</div>
	);
}