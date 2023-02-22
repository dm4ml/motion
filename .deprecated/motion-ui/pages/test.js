import Head from 'next/head';
import { Navbar, Button, Link, enableCursorHighlight, Dropdown, Text, Card, Radio } from "@nextui-org/react";
import { Layout } from '../components/layout';
import { useTheme } from '@nextui-org/react';

export default function Test() {
    const { theme } = useTheme();

    return (
        <Layout>
            <Head>
                <title>Test View</title>
            </Head>
        </Layout>
    );
}