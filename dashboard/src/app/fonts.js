// app/fonts.js

import {Rubik} from 'next/font/google'

const rubik = Rubik({subsets: ['latin'], variable: '--font-rubik'})
export const fonts = {
    rubik,
}