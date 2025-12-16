import { useEffect, useMemo, useRef, useState, useId, type ChangeEvent } from 'react'
import axios from 'axios'
import { useTranslation } from 'react-i18next'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useForm, useWatch } from 'react-hook-form'
import { z } from 'zod'
import { zodResolver } from '@hookform/resolvers/zod'
import { toast } from 'sonner'
import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Button } from '@/components/ui/button'
import { useSystemSettingsQuery } from '@/hooks/use-system-settings'
import {
  deleteSystemLogo,
  deleteBrowserIcon,
  updateSystemSettings,
  uploadBrowserIcon,
  uploadSystemLogo,
} from '@/services/system-settings'
import { ContentSection } from '@/features/settings/components/content-section'

const brandingFormSchema = z.object({
  name: z
    .string()
    .min(2, { message: 'System name must be at least 2 characters long.' })
    .max(60, { message: 'System name should be shorter than 60 characters.' }),
  tagline: z
    .string()
    .max(120, { message: 'Tagline should be shorter than 120 characters.' })
    .optional()
    .or(z.literal('')),
  logo: z.instanceof(File).optional().or(z.null()),
})

type BrandingFormValues = z.infer<typeof brandingFormSchema>

export function SystemBrandingConfigSection() {
  const { t } = useTranslation()
  const [logoOverride, setLogoOverride] = useState<string | null | undefined>(undefined)
  const [browserIconOverride, setBrowserIconOverride] = useState<string | null | undefined>(
    undefined,
  )
  const [browserIconSelectedName, setBrowserIconSelectedName] = useState<string | null>(null)
  const fileInputId = useId()
  const browserIconInputId = useId()
  const fileInputRef = useRef<HTMLInputElement | null>(null)
  const browserIconInputRef = useRef<HTMLInputElement | null>(null)
  const queryClient = useQueryClient()

  const {
    data: settings,
    isLoading,
    isFetching,
  } = useSystemSettingsQuery({
    refetchOnMount: false,
    refetchOnWindowFocus: false,
  })

  const form = useForm<BrandingFormValues>({
    resolver: zodResolver(brandingFormSchema),
    defaultValues: {
      name: 'Shadcn Admin',
      tagline: 'Vite + ShadcnUI Starter',
      logo: null,
    },
    values: settings
      ? {
          name: settings.name,
          tagline: settings.tagline ?? '',
          logo: null,
        }
      : undefined,
  })

  const watchedName = useWatch({
    control: form.control,
    name: 'name',
  })
  const currentInitial = useMemo(() => {
    return watchedName?.charAt(0)?.toUpperCase() ?? 'S'
  }, [watchedName])

  const updateMutation = useMutation({
    mutationFn: (values: BrandingFormValues) =>
      updateSystemSettings({
        name: values.name,
        tagline: values.tagline ? values.tagline : null,
      }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['system-settings'] })
      toast.success(t('systemSettings.branding.submit'))
    },
    onError: () => {
      toast.error(t('systemSettings.branding.submit') + ' failed')
    },
  })

  const uploadMutation = useMutation({
    mutationFn: (file: File) => uploadSystemLogo(file),
    onSuccess: async (_, file) => {
      toast.success(t('systemSettings.branding.logoLabel') + ' updated')
      const previewUrl = URL.createObjectURL(file)
      setLogoOverride(previewUrl)
      await queryClient.invalidateQueries({ queryKey: ['system-settings'] })
      setLogoOverride(undefined)
    },
    onError: (error: unknown) => {
      if (axios.isAxiosError?.(error)) {
        toast.error(error.response?.data?.message ?? 'Logo upload failed')
      } else {
        toast.error('Logo upload failed')
      }
    },
  })

  const uploadBrowserIconMutation = useMutation({
    mutationFn: (file: File) => uploadBrowserIcon(file),
    onSuccess: async (_, file) => {
      toast.success(t('systemSettings.branding.browserIconLabel') + ' updated')
      const previewUrl = URL.createObjectURL(file)
      setBrowserIconOverride(previewUrl)
      await queryClient.invalidateQueries({ queryKey: ['system-settings'] })
      setBrowserIconOverride(undefined)
      setBrowserIconSelectedName(null)
    },
    onError: (error: unknown) => {
      if (axios.isAxiosError?.(error)) {
        toast.error(error.response?.data?.message ?? 'Browser icon upload failed')
      } else {
        toast.error('Browser icon upload failed')
      }
    },
  })

  const removeLogoMutation = useMutation({
    mutationFn: () => deleteSystemLogo(),
    onSuccess: async () => {
      toast.success('Logo removed')
      setLogoOverride(null)
      await queryClient.invalidateQueries({ queryKey: ['system-settings'] })
      setLogoOverride(undefined)
    },
    onError: () => {
      toast.error('Failed to remove logo')
    },
  })

  const removeBrowserIconMutation = useMutation({
    mutationFn: () => deleteBrowserIcon(),
    onSuccess: async () => {
      toast.success('Browser icon removed')
      setBrowserIconOverride(null)
      await queryClient.invalidateQueries({ queryKey: ['system-settings'] })
      setBrowserIconOverride(undefined)
      setBrowserIconSelectedName(null)
    },
    onError: () => {
      toast.error('Failed to remove browser icon')
    },
  })

  useEffect(() => {
    if (!logoOverride || !logoOverride.startsWith?.('blob:')) return
    return () => {
      URL.revokeObjectURL(logoOverride)
    }
  }, [logoOverride])

  useEffect(() => {
    if (!browserIconOverride || !browserIconOverride.startsWith?.('blob:')) return
    return () => {
      URL.revokeObjectURL(browserIconOverride)
    }
  }, [browserIconOverride])

  function onBrandingSubmit(values: BrandingFormValues) {
    updateMutation.mutate(values)
  }

  function handleLogoChange(
    event: ChangeEvent<HTMLInputElement>,
    onChange: (file: File | null) => void,
  ) {
    const file = event.target.files?.[0] ?? null
    onChange(file)
    if (!file) {
      setLogoOverride(undefined)
      return
    }
    uploadMutation.mutate(file)
  }

  function handleBrowserIconChange(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0] ?? null
    setBrowserIconSelectedName(file?.name ?? null)
    if (!file) {
      setBrowserIconOverride(undefined)
      return
    }
    uploadBrowserIconMutation.mutate(file)
  }

  const watchedLogoFile = useWatch({
    control: form.control,
    name: 'logo',
  })
  const selectedFileName =
    watchedLogoFile && watchedLogoFile instanceof File ? watchedLogoFile.name : undefined
  const effectiveLogo =
    logoOverride === undefined ? settings?.logoUrl ?? null : logoOverride ?? null
  const hasLogo =
    logoOverride === undefined
      ? Boolean(settings?.logoUrl)
      : logoOverride !== null && logoOverride !== ''
  const effectiveBrowserIcon =
    browserIconOverride === undefined ? settings?.browserIconUrl ?? null : browserIconOverride ?? null
  const hasBrowserIcon =
    browserIconOverride === undefined
      ? Boolean(settings?.browserIconUrl)
      : browserIconOverride !== null && browserIconOverride !== ''

  const isSaving = updateMutation.isPending
  const isLogoBusy = uploadMutation.isPending || removeLogoMutation.isPending
  const isBrowserIconBusy =
    uploadBrowserIconMutation.isPending || removeBrowserIconMutation.isPending
  const showLoadingState = isLoading || isFetching

  return (
    <ContentSection
      title={t('systemSettings.branding.title')}
      desc={t('systemSettings.branding.description')}
      fullWidth
      scrollable={false}
    >
      <Form {...form}>
        <form className='space-y-6' onSubmit={form.handleSubmit(onBrandingSubmit)}>
          <div className='grid gap-4 lg:grid-cols-2 lg:items-stretch'>
            <FormField
              control={form.control}
              name='logo'
              render={({ field }) => (
                <FormItem className='flex h-full flex-col space-y-2'>
                  <div className='space-y-1'>
                    <FormLabel className='text-base'>{t('systemSettings.branding.logoLabel')}</FormLabel>
                    <p className='text-xs text-muted-foreground'>
                      {t('systemSettings.branding.logoHelp')}
                    </p>
                  </div>
                  <div className='flex flex-1 flex-wrap items-start gap-4 rounded-xl border bg-muted/30 p-4'>
                    <div className='flex h-20 w-20 items-center justify-center overflow-hidden rounded-xl border bg-background'>
                      {effectiveLogo ? (
                        <img src={effectiveLogo} alt='Logo preview' className='h-full w-full object-cover' />
                      ) : (
                        <span className='text-lg font-semibold text-muted-foreground'>
                          {currentInitial}
                        </span>
                      )}
                    </div>
                    <div className='space-y-2'>
                      <FormControl>
                        <>
                          <Input
                            id={fileInputId}
                            className='sr-only'
                            type='file'
                            accept='image/png,image/svg+xml'
                            name={field.name}
                            ref={(node) => {
                              field.ref(node)
                              fileInputRef.current = node
                            }}
                            onBlur={field.onBlur}
                            onChange={(event) => handleLogoChange(event, field.onChange)}
                            disabled={isLogoBusy || showLoadingState}
                          />
                          <Button
                            type='button'
                            disabled={isLogoBusy || showLoadingState}
                            className='h-9'
                            onClick={() => fileInputRef.current?.click()}
                          >
                            {hasLogo
                              ? t('systemSettings.branding.replaceLogo')
                              : t('systemSettings.branding.uploadLogo')}
                          </Button>
                        </>
                      </FormControl>
                      {selectedFileName && (
                        <p className='text-xs text-muted-foreground'>{selectedFileName}</p>
                      )}
                      <FormDescription className='text-xs'>
                        {t('systemSettings.branding.logoHelp')}
                      </FormDescription>
                      {settings?.logoUrl && (
                        <Button
                          type='button'
                          variant='ghost'
                          className='px-0 text-destructive'
                          disabled={isLogoBusy}
                          onClick={() => removeLogoMutation.mutate()}
                        >
                          {removeLogoMutation.isPending ? 'Removing...' : t('common.remove')}
                        </Button>
                      )}
                    </div>
                  </div>
                  <FormMessage />
                </FormItem>
              )}
            />

            <div className='flex h-full flex-col space-y-2'>
              <div className='space-y-1'>
                <FormLabel className='text-base'>{t('systemSettings.branding.browserIconLabel')}</FormLabel>
                <p className='text-xs text-muted-foreground'>
                  {t('systemSettings.branding.browserIconDescription')}
                </p>
              </div>
              <div className='flex flex-1 flex-wrap items-start gap-4 rounded-xl border bg-muted/30 p-4'>
                <div className='flex h-12 w-12 items-center justify-center overflow-hidden rounded-xl border bg-background'>
                  {effectiveBrowserIcon ? (
                    <img
                      src={effectiveBrowserIcon}
                      alt='Browser icon preview'
                      className='h-full w-full object-contain'
                    />
                  ) : (
                    <span className='text-xs font-semibold text-muted-foreground'>{currentInitial}</span>
                  )}
                </div>
                <div className='space-y-2'>
                  <FormControl>
                    <>
                      <Input
                        id={browserIconInputId}
                        className='sr-only'
                        type='file'
                        accept='image/png,image/svg+xml,image/x-icon,.ico'
                        ref={browserIconInputRef}
                        onChange={handleBrowserIconChange}
                        disabled={isBrowserIconBusy || showLoadingState}
                      />
                      <Button
                        type='button'
                        disabled={isBrowserIconBusy || showLoadingState}
                        className='h-9'
                        onClick={() => browserIconInputRef.current?.click()}
                      >
                        {hasBrowserIcon
                          ? t('systemSettings.branding.replaceBrowserIcon')
                          : t('systemSettings.branding.uploadBrowserIcon')}
                      </Button>
                    </>
                  </FormControl>
                  {browserIconSelectedName && (
                    <p className='text-xs text-muted-foreground'>{browserIconSelectedName}</p>
                  )}
                  <FormDescription className='text-xs'>
                    {t('systemSettings.branding.browserIconHelp')}
                  </FormDescription>
                  {settings?.browserIconUrl && (
                    <Button
                      type='button'
                      variant='ghost'
                      className='px-0 text-destructive'
                      disabled={isBrowserIconBusy}
                      onClick={() => removeBrowserIconMutation.mutate()}
                    >
                      {removeBrowserIconMutation.isPending ? 'Removing...' : t('common.remove')}
                    </Button>
                  )}
                </div>
              </div>
            </div>
          </div>

          <FormField
            control={form.control}
            name='name'
            render={({ field }) => (
              <FormItem>
                <FormLabel>{t('systemSettings.branding.nameLabel')}</FormLabel>
                <FormControl>
                  <Input {...field} placeholder='Acme AI Console' />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name='tagline'
            render={({ field }) => (
              <FormItem>
                <FormLabel>{t('systemSettings.branding.taglineLabel')}</FormLabel>
                <FormControl>
                  <Textarea {...field} rows={3} placeholder='Where your AI workflows come to life' />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />

          <div className='flex justify-start'>
            <Button type='submit' disabled={isSaving || showLoadingState}>
              {isSaving ? t('common.saving') : t('systemSettings.branding.submit')}
            </Button>
          </div>

        </form>
      </Form>
    </ContentSection>
  )
}
