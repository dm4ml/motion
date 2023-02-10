import { Card, Spacer, Container, Row, Text } from "@nextui-org/react";

export function Notebook() {

    return (
        <Container md>
            <Spacer y={1} />
            <Card variant="flat" >
                <Card.Body>
                    <Row justify="center" align="center">
                        <Text h6 size={15} color="black" css={{ m: 0 }}>
                            Hello world!
                        </Text>
                    </Row>
                </Card.Body>
            </Card >
            <Spacer y={1} />
            <Card>
                <Card.Body>
                    <Row justify="center" align="center">
                        <Text h6 size={15} color="black" css={{ m: 0 }}>
                            Hello world!
                        </Text>
                    </Row>
                </Card.Body>
            </Card>
        </Container>
    );


}