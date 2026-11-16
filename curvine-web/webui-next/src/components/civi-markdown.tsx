import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'

import { cn } from '@/lib/utils'

/**
 * Markdown renderer for civi chat answers.
 *
 * The model is instructed to answer in GFM Markdown (tables, fenced code),
 * so every element gets explicit console-styled mapping — there is no
 * typography plugin in this app, and raw browser defaults would look out of
 * place on both light and dark themes.
 */
export function CiviMarkdown({ content, className }: { content: string; className?: string }) {
  return (
    <div className={cn('space-y-2 text-sm leading-relaxed break-words', className)}>
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          p: ({ children }) => <p className='whitespace-pre-wrap break-words'>{children}</p>,
          h1: ({ children }) => (
            <h2 className='mt-3 text-base font-semibold first:mt-0'>{children}</h2>
          ),
          h2: ({ children }) => (
            <h3 className='mt-3 text-[15px] font-semibold first:mt-0'>{children}</h3>
          ),
          h3: ({ children }) => (
            <h4 className='mt-2 text-sm font-semibold first:mt-0'>{children}</h4>
          ),
          h4: ({ children }) => (
            <h5 className='mt-2 text-sm font-semibold first:mt-0'>{children}</h5>
          ),
          ul: ({ children }) => <ul className='list-disc space-y-1 ps-5'>{children}</ul>,
          ol: ({ children }) => <ol className='list-decimal space-y-1 ps-5'>{children}</ol>,
          li: ({ children }) => <li className='break-words'>{children}</li>,
          a: ({ href, children }) => (
            <a
              href={href}
              target='_blank'
              rel='noreferrer'
              className='text-primary underline underline-offset-2'
            >
              {children}
            </a>
          ),
          blockquote: ({ children }) => (
            <blockquote className='border-s-2 border-border ps-3 text-muted-foreground'>
              {children}
            </blockquote>
          ),
          strong: ({ children }) => <strong className='font-semibold'>{children}</strong>,
          // Inline code: a fragment child inside `pre` is a code block —
          // distinguish by checking whether the renderer wraps it already.
          code: ({ className: cls, children }) => {
            const isBlock = /language-/.test(cls ?? '') || String(children).includes('\n')
            if (isBlock) {
              return (
                <code className={cn('block overflow-x-auto font-mono text-xs', cls)}>
                  {children}
                </code>
              )
            }
            return (
              <code className='rounded bg-muted px-1 py-0.5 font-mono text-[12px]'>{children}</code>
            )
          },
          pre: ({ children }) => (
            <pre className='overflow-x-auto rounded-lg border bg-muted/60 p-2.5 font-mono text-xs'>
              {children}
            </pre>
          ),
          table: ({ children }) => (
            <div className='overflow-x-auto rounded-lg border'>
              <table className='w-full border-collapse text-xs'>{children}</table>
            </div>
          ),
          thead: ({ children }) => <thead className='bg-muted/60'>{children}</thead>,
          th: ({ children }) => (
            <th className='border-b px-2.5 py-1.5 text-start font-semibold'>{children}</th>
          ),
          td: ({ children }) => (
            <td className='border-b px-2.5 py-1.5 align-top last:border-b-0'>{children}</td>
          ),
          tr: ({ children }) => <tr className='last:border-b-0'>{children}</tr>
        }}
      >
        {content}
      </ReactMarkdown>
    </div>
  )
}
