import Head from 'next/head';
import { Navbar, Button, Link, enableCursorHighlight, Dropdown, Text, Card, Radio } from "@nextui-org/react";
import { Layout } from '../components/layout';

import { useTheme } from '@nextui-org/react';

export default function Home() {
  const { theme } = useTheme();

  return (
    <Layout>
      <Head>
        <title>Explore View</title>
        {/* <script src="https://cdn.jsdelivr.net/pyodide/v0.22.1/full/pyodide.js"></script> */}
      </Head>
    </Layout>
  );
}