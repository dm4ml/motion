import ThemeProvider from './theme-provider';
import './globals.css'
import { fonts } from './fonts'

export const metadata = {
  title: 'Motion dashboard',
  description: 'Dashboard for Motion',
}

export default function RootLayout({ children }) {
  return (
    <html lang="en" className={fonts.rubik.variable}>
      <body>
      <ThemeProvider>{children}</ThemeProvider>
      </body>
    </html>
  )
}
