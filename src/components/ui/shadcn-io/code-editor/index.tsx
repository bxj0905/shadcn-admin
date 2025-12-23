'use client';

import * as React from 'react';
import AceEditor from 'react-ace';
import { Check, Copy } from 'lucide-react';

import { cn } from '@/lib/utils';
import { Button } from '@/components/ui/button';
import '@/styles/ace-twilight-override.css';

// Ace 语言 & 主题（按照 docs/editor 中的配置，使用 twilight 主题）
import 'ace-builds/src-noconflict/mode-python';
import 'ace-builds/src-noconflict/theme-twilight';
import 'ace-builds/src-noconflict/ext-language_tools';

type CopyButtonProps = {
  content: string;
  className?: string;
  onCopy?: (content: string) => void;
};

function CopyButton({ content, className, onCopy }: CopyButtonProps) {
  const [copied, setCopied] = React.useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(content);
      setCopied(true);
      onCopy?.(content);
      setTimeout(() => setCopied(false), 1500);
    } catch (err) {
      console.error('Failed to copy text: ', err);
    }
  };

  return (
    <Button
      type="button"
      size="icon"
      variant="ghost"
      onClick={handleCopy}
      className={cn('h-8 w-8 p-0', className)}
    >
      {copied ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
    </Button>
  );
}

export type CodeEditorProps = {
  value?: string;
  defaultValue?: string;
  onChange?: (value: string) => void;
  language?: 'python' | string;
  height?: number | string; // 容器高度，默认跟随父容器
  placeholder?: string;
  className?: string;
};

export function CodeEditor({
  value,
  defaultValue = '',
  onChange,
  language = 'python',
  height = '100%',
  placeholder,
  className,
}: CodeEditorProps) {
  const [code, setCode] = React.useState<string>(value ?? defaultValue);

  // 加载 Roboto Mono 字体（和 docs/editor 一致）
  React.useEffect(() => {
    const id = 'roboto-mono-font';
    if (!document.getElementById(id)) {
      const link = document.createElement('link');
      link.id = id;
      link.rel = 'stylesheet';
      link.href = 'https://fonts.googleapis.com/css?family=Roboto+Mono';
      document.head.appendChild(link);
    }
  }, []);

  // 受控 -> 内部状态同步
  React.useEffect(() => {
    if (typeof value === 'string') {
      setCode(value);
    }
  }, [value]);

  const aceHeight = typeof height === 'number' ? `${height}px` : height;

  return (
    <div
      className={cn(
        'relative rounded-md border bg-black',
        className,
      )}
      style={{
        width: '100%',
        height: typeof height === 'number' ? `${height}px` : height,
        fontFamily: "'Roboto Mono', ui-monospace, SFMono-Regular, Menlo, monospace",
      }}
    >
      <div className="h-full">
        <AceEditor
          mode={language}
          theme="twilight"
          width="100%"
          height={aceHeight}
          value={code}
          onChange={(next) => {
            setCode(next);
            onChange?.(next);
          }}
          placeholder={placeholder}
          name="prefect-flow-code"
          fontSize={14}
          showPrintMargin={false}
          showGutter={true}
          highlightActiveLine={true}
          editorProps={{ $blockScrolling: true }}
          setOptions={{
            enableBasicAutocompletion: true,
            enableLiveAutocompletion: true,
            enableSnippets: true,
            showLineNumbers: true,
            tabSize: 4,
            useWorker: false,
          }}
          style={{
            backgroundColor: 'black',
          }}
        />
      </div>
    </div>
  );
}

export { CopyButton };

