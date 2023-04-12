import svelte from 'rollup-plugin-svelte';
import resolve from 'rollup-plugin-node-resolve';
import commonjs from 'rollup-plugin-commonjs';
import css from 'rollup-plugin-css-only';

export default {
    input: 'main.js',
    output: {
        file: 'public/bundle.js',
        format: 'iife',
        name: 'app',
    },
    plugins: [
        svelte(),
        resolve({
            browser: true,
            dedupe: ['svelte'],
        }),
        commonjs(),
        css({ output: 'bundle.css' }),
    ],
};