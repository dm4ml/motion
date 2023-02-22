import { Navbar, Button, Link, enableCursorHighlight, Dropdown, Text, Card, Radio } from "@nextui-org/react";
import { IconPlayerPlayFilled, IconPlus, IconRefresh, IconPackage } from '@tabler/icons-react';
import { useRouter } from 'next/router';
import React from "react";
import { useTheme as useNextTheme } from 'next-themes'
import { Switch, useTheme } from '@nextui-org/react'

export function Header({ onNewClick, explore, onRun }) {
    const [selected, setSelected] = React.useState(new Set(["type"]));
    const { asPath } = useRouter();


    const selectedValue = React.useMemo(
        () => Array.from(selected).join(", ").replaceAll("_", " "),
        [selected]
    );

    const { setTheme } = useNextTheme();
    const { isDark, type } = useTheme();

    let runButtonText = explore === true ? "Restart pipeline" : "Run pipeline";

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
                                    tt: "capitalize", minWidth: "150px",
                                    fontWeight: "$semibold",
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
                        css={{
                            $$dropdownMenuWidth: "300px",
                            $$dropdownItemHeight: "60px",
                            "& .nextui-dropdown-item": {
                                py: "$4",
                                // dropdown item left icon
                                // svg: {
                                //     color: "$secondary",
                                //     mr: "$4",
                                // },
                                // dropdown item title
                                "& .nextui-dropdown-item-content": {
                                    w: "100%",
                                    fontWeight: "$semibold",
                                },
                            },
                        }}
                    >
                        <Dropdown.Item
                            key="type" description="Create a new featureType, labelType, or returnType." showFullDescription
                        >
                            Type
                        </Dropdown.Item>
                        <Dropdown.Item
                            key="transform" description="Create a new, stateful transform for the pipeline." showFullDescription
                        >
                            Transform
                        </Dropdown.Item>
                        <Dropdown.Item
                            key="free" description="Interact with intermediate state (read-only)." showFullDescription
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
                <Switch
                    checked={isDark}
                    onChange={(e) => setTheme(e.target.checked ? 'dark' : 'light')}
                />
                <Navbar.Item>
                    <Button
                        icon={explore === true ? <IconRefresh /> : <IconPlayerPlayFilled />} ghost auto onPress={onRun}  >
                        {runButtonText}
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