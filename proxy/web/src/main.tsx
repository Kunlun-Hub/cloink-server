import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import { ErrorPage } from './ErrorPage.tsx'
import { getData } from '@/data'
import { I18nProvider } from '@/i18n/I18nProvider'

const data = getData()

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <I18nProvider>
      {data.page === 'error' && data.error ? (
        <ErrorPage {...data.error} />
      ) : (
        <App />
      )}
    </I18nProvider>
  </StrictMode>,
)
