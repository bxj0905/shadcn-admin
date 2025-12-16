import { useRef, useState } from 'react'
import { z } from 'zod'
import { zodResolver } from '@hookform/resolvers/zod'
import { useForm, FormProvider, useWatch } from 'react-hook-form'
import { useTranslation } from 'react-i18next'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Loader2, ShieldCheck } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import {
  Form,
  FormField,
  FormItem,
  FormLabel,
  FormControl,
  FormMessage,
  FormDescription,
} from '@/components/ui/form'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Badge } from '@/components/ui/badge'
import {
  Select,
  SelectTrigger,
  SelectContent,
  SelectItem,
  SelectValue,
} from '@/components/ui/select'
import { ContentSection } from '@/features/settings/components/content-section'
import { DatePicker } from '@/components/date-picker'
import { useSystemLicenseQuery } from '@/hooks/use-system-license'
import {
  deleteLicenseFile,
  updateSystemLicense,
  uploadLicenseFile,
} from '@/services/system-settings'

const STATUS_VALUES = ['DRAFT', 'ACTIVE', 'EXPIRED'] as const

const licenseFormSchema = z.object({
  status: z.enum(STATUS_VALUES),
  licenseName: z
    .string()
    .max(200, { message: 'License name should be shorter than 200 characters.' })
    .optional()
    .or(z.literal('')),
  issuer: z
    .string()
    .max(200, { message: 'Issuer should be shorter than 200 characters.' })
    .optional()
    .or(z.literal('')),
  validFrom: z.date().optional().nullable(),
  validTo: z.date().optional().nullable(),
  notes: z
    .string()
    .max(1000, { message: 'Notes should be shorter than 1000 characters.' })
    .optional()
    .or(z.literal('')),
})

type LicenseFormValues = z.infer<typeof licenseFormSchema>

const statusBadgeVariants: Record<(typeof STATUS_VALUES)[number], 'outline' | 'default' | 'destructive'> = {
  DRAFT: 'outline',
  ACTIVE: 'default',
  EXPIRED: 'destructive',
}

function safeDate(value?: string | null) {
  if (!value) return undefined
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? undefined : date
}

export function SystemLicenseSection() {
  const { t } = useTranslation()
  const queryClient = useQueryClient()
  const fileInputRef = useRef<HTMLInputElement | null>(null)
  const [lastUploadedName, setLastUploadedName] = useState<string | null>(null)

  const {
    data: license,
    isLoading,
    isFetching,
  } = useSystemLicenseQuery({
    refetchOnMount: false,
    refetchOnWindowFocus: false,
  })

  const form = useForm<LicenseFormValues>({
    resolver: zodResolver(licenseFormSchema),
    defaultValues: {
      status: 'DRAFT',
      licenseName: '',
      issuer: '',
      validFrom: undefined,
      validTo: undefined,
      notes: '',
    },
    values: license
      ? {
          status: license.status,
          licenseName: license.licenseName ?? '',
          issuer: license.issuer ?? '',
          validFrom: safeDate(license.validFrom),
          validTo: safeDate(license.validTo),
          notes: license.notes ?? '',
        }
      : undefined,
  })

  const watchedStatus = useWatch({ control: form.control, name: 'status' }) ?? 'DRAFT'

  const updateMutation = useMutation({
    mutationFn: (values: LicenseFormValues) =>
      updateSystemLicense({
        status: values.status,
        licenseName: values.licenseName?.trim() ? values.licenseName.trim() : null,
        issuer: values.issuer?.trim() ? values.issuer.trim() : null,
        validFrom: values.validFrom ? values.validFrom.toISOString() : null,
        validTo: values.validTo ? values.validTo.toISOString() : null,
        notes: values.notes?.trim() ? values.notes.trim() : null,
      }),
    onSuccess: async () => {
      toast.success(t('systemSettings.license.toast.saveSuccess'))
      await queryClient.invalidateQueries({ queryKey: ['system-license'] })
    },
    onError: () => {
      toast.error(t('systemSettings.license.toast.saveError'))
    },
  })

  const uploadMutation = useMutation({
    mutationFn: (file: File) => uploadLicenseFile(file),
    onSuccess: async () => {
      toast.success(t('systemSettings.license.toast.fileUploaded'))
      setLastUploadedName(null)
      await queryClient.invalidateQueries({ queryKey: ['system-license'] })
    },
    onError: () => {
      toast.error(t('systemSettings.license.toast.fileUploadError'))
    },
  })

  const deleteMutation = useMutation({
    mutationFn: () => deleteLicenseFile(),
    onSuccess: async () => {
      toast.success(t('systemSettings.license.toast.fileRemoved'))
      await queryClient.invalidateQueries({ queryKey: ['system-license'] })
    },
    onError: () => {
      toast.error(t('systemSettings.license.toast.fileRemoveError'))
    },
  })

  const isBusy = isLoading || isFetching
  const isSaving = updateMutation.isPending
  const isUploading = uploadMutation.isPending
  const isDeleting = deleteMutation.isPending
  const hasFile = Boolean(license?.fileUrl)
  let lastUpdated: string | null = null
  if (license?.updatedAt) {
    const parsed = new Date(license.updatedAt)
    if (!Number.isNaN(parsed.getTime())) {
      lastUpdated = parsed.toLocaleString()
    }
  }

  function onSubmit(values: LicenseFormValues) {
    updateMutation.mutate(values)
  }

  function handleFileChange(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0]
    if (!file) {
      setLastUploadedName(null)
      return
    }
    setLastUploadedName(file.name)
    uploadMutation.mutate(file)
  }

  const docsHref = 'https://docs.shadcn-admin.com/licensing'
  const statusLabel = t(`systemSettings.license.statuses.${watchedStatus.toLowerCase()}`)

  return (
    <ContentSection
      title={t('systemSettings.license.title')}
      desc={t('systemSettings.license.description')}
      fullWidth
      scrollable={false}
    >
      <div className='space-y-6'>
        <div className='flex flex-wrap items-center gap-3 rounded-2xl border border-dashed bg-muted/40 p-4'>
          <Badge variant={statusBadgeVariants[watchedStatus]} className='uppercase tracking-wide'>
            {statusLabel}
          </Badge>
          <div className='text-sm text-muted-foreground'>
            {license
              ? t('systemSettings.license.statusDescription', { status: statusLabel })
              : t('systemSettings.license.placeholder')}
            {lastUpdated && (
              <span className='ml-2 text-xs text-muted-foreground'>
                {t('systemSettings.license.lastUpdated', { date: lastUpdated })}
              </span>
            )}
          </div>
          <div className='ms-auto flex flex-wrap gap-2'>
            <Button type='button' variant='secondary' asChild>
              <a href={docsHref} target='_blank' rel='noreferrer'>
                <ShieldCheck className='me-2 h-4 w-4' />
                {t('systemSettings.license.docs')}
              </a>
            </Button>
          </div>
        </div>

        <div className='grid gap-6 lg:grid-cols-[minmax(0,2fr)_minmax(280px,1fr)]'>
          <FormProvider {...form}>
            <Form {...form}>
              <form className='space-y-6' onSubmit={form.handleSubmit(onSubmit)}>
                <div className='grid gap-4 md:grid-cols-2'>
                  <FormField
                    control={form.control}
                    name='status'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>{t('systemSettings.license.fields.status')}</FormLabel>
                        <Select value={field.value} onValueChange={field.onChange} disabled={isBusy || isSaving}>
                          <FormControl>
                            <SelectTrigger>
                              <SelectValue placeholder={t('systemSettings.license.statusPlaceholder')} />
                            </SelectTrigger>
                          </FormControl>
                          <SelectContent>
                            {STATUS_VALUES.map((value) => (
                              <SelectItem key={value} value={value}>
                                {t(`systemSettings.license.statuses.${value.toLowerCase()}`)}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        <FormMessage />
                      </FormItem>
                    )}
                  />

                  <FormField
                    control={form.control}
                    name='licenseName'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>{t('systemSettings.license.fields.licenseName')}</FormLabel>
                        <FormControl>
                          <Input placeholder={t('systemSettings.license.placeholders.licenseName')} {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />

                  <FormField
                    control={form.control}
                    name='issuer'
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>{t('systemSettings.license.fields.issuer')}</FormLabel>
                        <FormControl>
                          <Input placeholder={t('systemSettings.license.placeholders.issuer')} {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />

                  <div className='grid gap-4 sm:grid-cols-2'>
                    <FormField
                      control={form.control}
                      name='validFrom'
                      render={({ field }) => (
                        <FormItem className='flex flex-col'>
                          <FormLabel>{t('systemSettings.license.fields.validFrom')}</FormLabel>
                          <FormControl>
                            <DatePicker
                              selected={field.value ?? undefined}
                              onSelect={(date) => field.onChange(date ?? undefined)}
                              placeholder={t('systemSettings.license.placeholders.date')}
                            />
                          </FormControl>
                          <FormMessage />
                        </FormItem>
                      )}
                    />

                    <FormField
                      control={form.control}
                      name='validTo'
                      render={({ field }) => (
                        <FormItem className='flex flex-col'>
                          <FormLabel>{t('systemSettings.license.fields.validTo')}</FormLabel>
                          <FormControl>
                            <DatePicker
                              selected={field.value ?? undefined}
                              onSelect={(date) => field.onChange(date ?? undefined)}
                              placeholder={t('systemSettings.license.placeholders.date')}
                            />
                          </FormControl>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                  </div>

                  <FormField
                    control={form.control}
                    name='notes'
                    render={({ field }) => (
                      <FormItem className='md:col-span-2'>
                        <FormLabel>{t('systemSettings.license.fields.notes')}</FormLabel>
                        <FormControl>
                          <Textarea
                            rows={4}
                            placeholder={t('systemSettings.license.placeholders.notes')}
                            {...field}
                          />
                        </FormControl>
                        <FormDescription>{t('systemSettings.license.notesHelp')}</FormDescription>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                </div>

                <div className='flex flex-wrap gap-3'>
                  <Button type='submit' disabled={isSaving || isBusy}>
                    {isSaving && <Loader2 className='me-2 h-4 w-4 animate-spin' />}
                    {t('systemSettings.license.actions.save')}
                  </Button>
                </div>
              </form>
            </Form>
          </FormProvider>

          <div className='space-y-4 rounded-2xl border bg-muted/30 p-4'>
            <div className='space-y-1'>
              <p className='text-base font-medium'>{t('systemSettings.license.file.title')}</p>
              <p className='text-sm text-muted-foreground'>
                {t('systemSettings.license.file.description')}
              </p>
            </div>
            <div className='rounded-xl border bg-background p-4'>
              {hasFile ? (
                <div className='space-y-2 text-sm'>
                  <p className='font-medium'>{license?.fileUrl?.split('/').pop()}</p>
                  <p className='text-muted-foreground'>
                    {t('systemSettings.license.file.format', {
                      format: license?.fileFormat?.toUpperCase() ?? '—',
                    })}
                  </p>
                  <div className='flex flex-wrap gap-2'>
                    <Button type='button' variant='secondary' asChild size='sm'>
                      <a href={license?.fileUrl ?? '#'} target='_blank' rel='noreferrer'>
                        {t('systemSettings.license.file.download')}
                      </a>
                    </Button>
                    <Button
                      type='button'
                      variant='ghost'
                      size='sm'
                      className='text-destructive'
                      disabled={isDeleting}
                      onClick={() => deleteMutation.mutate()}
                    >
                      {isDeleting && <Loader2 className='me-1 h-3.5 w-3.5 animate-spin' />}
                      {t('systemSettings.license.file.remove')}
                    </Button>
                  </div>
                </div>
              ) : (
                <p className='text-sm text-muted-foreground'>
                  {t('systemSettings.license.file.empty')}
                </p>
              )}
            </div>
            <div className='space-y-2'>
              <input
                ref={fileInputRef}
                type='file'
                accept='application/pdf,application/zip,application/x-zip-compressed,.zip'
                className='hidden'
                onChange={handleFileChange}
                disabled={isUploading}
              />
              <Button
                type='button'
                variant='outline'
                onClick={() => fileInputRef.current?.click()}
                disabled={isUploading}
                className='w-full'
              >
                {isUploading && <Loader2 className='me-2 h-4 w-4 animate-spin' />}
                {hasFile
                  ? t('systemSettings.license.file.replace')
                  : t('systemSettings.license.file.upload')}
              </Button>
              {lastUploadedName && (
                <p className='text-xs text-muted-foreground'>{lastUploadedName}</p>
              )}
              <p className='text-xs text-muted-foreground'>
                {t('systemSettings.license.file.allowed')}
              </p>
            </div>
          </div>
        </div>
      </div>
    </ContentSection>
  )
}
