import Head from 'next/head';
import { Navbar, Button, Link, enableCursorHighlight, Dropdown, Text, Card, Radio } from "@nextui-org/react";
import { Layout } from '../components/layout';
import { Header } from '../components/header';
import { Notebook } from '../components/notebook';
import { useTheme } from '@nextui-org/react';

export default function Home() {
  const { theme } = useTheme();

  return (
    <Layout>
      <Head>
        <title>Nothing</title>
      </Head>
      <Header />
      <Notebook />
    </Layout>
  );
}