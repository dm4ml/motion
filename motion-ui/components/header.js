import { Navbar, Button, Link, enableCursorHighlight, Dropdown, Text, Card, Radio } from "@nextui-org/react";
import { IconPlayerPlayFilled, IconPlus, IconChevronDown, IconPackage } from '@tabler/icons-react';
import { useRouter } from 'next/router';
import React from "react";

export function Header({ onNewClick }) {
    const [selected, setSelected] = React.useState(new Set(["type"]));
    const { asPath } = useRouter();


    const selectedValue = React.useMemo(
        () => Array.from(selected).join(", ").replaceAll("_", " "),
        [selected]
    );

    return (
        <Navbar isBordered variant="floating" maxWidth="fluid" borderWeight="bold">
            <Navbar.Content hideIn="xs">
                <Dropdown>
                    <Navbar.Item>
                        <Button.Group>
                            <Button
                                light
                                auto
                                icon={<IconPlus />}
                                onPress={() => onNewClick(selectedValue)}
                            />
                            <Dropdown.Button
                                light
                                css={{
                                    // px: 0,
                                    dflex: "left",
                                    tt: "capitalize", minWidth: "150px"
                                }}
                            // iconRight={<IconChevronDown />}
                            >
                                {selectedValue}
                            </Dropdown.Button>
                        </Button.Group>
                    </Navbar.Item>
                    <Dropdown.Menu
                        selectionMode="single"
                        disallowEmptySelection
                        selectedKeys={selected}
                        onSelectionChange={setSelected}
                    >
                        <Dropdown.Item
                            key="type"
                        >
                            Type
                        </Dropdown.Item>
                        <Dropdown.Item
                            key="transform"
                        >
                            Transform
                        </Dropdown.Item>
                        <Dropdown.Item
                            key="free"
                        >
                            Free
                        </Dropdown.Item>
                    </Dropdown.Menu>
                </Dropdown>
            </Navbar.Content>
            <Navbar.Content enableCursorHighlight hideIn="xs" variant="underline" >
                <Navbar.Link isActive={asPath === "/"} href="/">Explore</Navbar.Link>
                <Navbar.Link isActive={asPath === "/test"} href="/test">Test</Navbar.Link>
            </Navbar.Content>
            <Navbar.Content>
                <Navbar.Item>
                    <Button
                        icon={<IconPlayerPlayFilled />} ghost auto >
                        Run pipeline
                    </Button>
                </Navbar.Item>
                <Navbar.Item>
                    <Button
                        icon={<IconPackage />} auto ghost  >
                        Show imports
                    </Button>
                </Navbar.Item>
            </Navbar.Content>
        </Navbar>
    );
}