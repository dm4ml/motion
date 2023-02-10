// import '../styles/globals.css';

// 1. import `NextUIProvider` component
import { createTheme, NextUIProvider } from "@nextui-org/react"
// import 'codemirror/lib/codemirror.css';
import { SSRProvider } from '@react-aria/ssr';

// 2. Call `createTheme` and pass your custom theme values
const theme = createTheme({
    type: "light", // it could be "light" or "dark"
    theme: {
        colors: {
            typeColor: '#FFDFBD',
            transformColor: '#94E2FF',
            darkColor: '#003E54',
            linkColor: '#5369FC',
            greyColor: '#D9D9D9',

            typeColorAlpha: 'rgba(255, 223, 189, 0.4)',
            transformColorAlpha: 'rgba(148, 226, 255, 0.4)',

            primary100: '#DCE2FE',
            primary200: '#BAC6FE',
            primary300: '#97A7FE',
            primary400: '#7D90FD',
            primary500: '#5369FC',
            primary600: '#3C4ED8',
            primary700: '#2938B5',


            // brand colors
            primaryLight: '$primary200',
            primaryLightHover: '$primary300',
            primaryLightActive: '$primary400',
            primaryLightContrast: '$primary600',
            primary: '$primary500',
            primaryBorder: '$primary500',
            primaryBorderHover: '$primary600',
            primarySolidHover: '$primary700',
            primarySolidContrast: '$white',
            primaryShadow: '$primary500',

            // gradient: 'linear-gradient(112deg, $blue100 -25%, $pink500 -10%, $purple500 80%)',
            link: '$linkColor',

            // you can also create your own color
            myColor: '#ff4ecd'

            // ...  more colors
        },
        space: {},
        fonts: {}
    }
})


export default function App({ Component, pageProps }) {
    return <NextUIProvider>< Component {...pageProps} /></NextUIProvider >;
}