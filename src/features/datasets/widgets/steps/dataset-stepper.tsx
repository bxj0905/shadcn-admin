import React from 'react'
import { Check } from 'lucide-react'
import { cn } from '@/lib/utils'

type Step = {
  id: number
  title: string
}

type DatasetStepperProps = {
  steps: Step[]
  currentStep: number
}

export function DatasetStepper({ steps, currentStep }: DatasetStepperProps) {
  return (
    <div className='mt-1 mb-2 flex items-center justify-between border-b pb-5'>
      {steps.map((s, index, arr) => {
        const isDone = currentStep > s.id
        const isActive = currentStep === s.id

        return (
          <div key={s.id} className='relative flex flex-1 flex-col items-center'>
            <div
              className={cn(
                'flex h-10 w-10 items-center justify-center rounded-full text-sm font-semibold transition-colors duration-300',
                isDone
                  ? 'bg-primary text-primary-foreground'
                  : isActive
                    ? 'bg-primary/90 text-primary-foreground'
                    : 'bg-muted text-muted-foreground',
              )}
            >
              {isDone ? <Check className='h-5 w-5' /> : s.id}
            </div>
            <div
              className={cn(
                'mt-2 text-center text-sm font-medium',
                currentStep >= s.id ? 'text-foreground' : 'text-muted-foreground',
              )}
            >
              {s.title}
            </div>

            {index < arr.length - 1 && (
              <div
                className={cn(
                  'absolute left-[calc(50%+20px)] top-5 h-0.5 w-[calc(100%-40px)] -translate-y-1/2 bg-muted transition-colors duration-300',
                  currentStep > s.id && 'bg-primary/60',
                )}
              />
            )}
          </div>
        )
      })}
    </div>
  )
}

