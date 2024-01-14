"use client";

import { useState } from 'react';
import {
  Box,
  Flex,
  Input,
  VStack,
  HStack,
  Text,
  Link,
  useColorModeValue,
  Popover,
  PopoverTrigger,
  PopoverContent,
  PopoverArrow,
  PopoverCloseButton,
  PopoverHeader,
  PopoverBody,
  Button,
  IconButton
} from '@chakra-ui/react';
import { EditIcon } from '@chakra-ui/icons';
import NextLink from 'next/link'

const components = [
  { name: 'Component 1', instances: [{name: 'Instance 1', details: 'Details about Instance 1'}, {name: 'Instance 2', details: 'Details about Instance 2'}] },
  { name: 'Component 2', instances: [{name: 'Instance A', details: 'Details about Instance A'}, {name: 'Instance B', details: 'Details about Instance B'}] },
  // ... more components
];

export default function Home() {
  const [selectedComponent, setSelectedComponent] = useState(components[0]);
  const [searchTerm, setSearchTerm] = useState('');
  const [editingInstance, setEditingInstance] = useState(null);
  const [editDetails, setEditDetails] = useState('');

  // Function to start editing an instance
  const startEditing = (instance) => {
    setEditingInstance(instance);
    setEditDetails(instance.details);
  };

  // Function to submit the edited details
  const submitEdit = (instance) => {
    instance.details = editDetails;
    setEditingInstance(null);
  };


  return (
    <Flex>
      <VStack
        w="20%"
        p="4"
        bg={useColorModeValue('gray.100', 'gray.700')}
        height="100vh"
      >
        {components.map((component, index) => (
          <Link href="#" 
          key={index} as={NextLink}
            color={selectedComponent === component ? 'red.500' : 'blue.400'} // Highlight selected component
            _hover={{ color: 'blue.600' }}
            onClick={() => setSelectedComponent(component)}
          >
            {component.name}
          </Link>
        ))}
      </VStack>

      <Box w="80%" p="4">
        {selectedComponent && (
          <>
            <Input 
              placeholder='Search for instances...'
              mb="4"
              onChange={(e) => setSearchTerm(e.target.value)}
            />
            <VStack>
              {selectedComponent.instances
                .filter(instance =>
                  instance.name.toLowerCase().includes(searchTerm.toLowerCase())
                )
                .map((instance, index) => (
                  <Popover key={index} isLazy>
                    <PopoverTrigger>
                      <HStack bg={useColorModeValue('gray.200', 'gray.600')} p="4" borderRadius="md" cursor="pointer">
                        <Text>{instance.name}</Text>
                      </HStack>
                    </PopoverTrigger>
                    <PopoverContent>
                      <PopoverArrow />
                      <PopoverCloseButton />
                      <PopoverHeader>
                        {instance.name}
                        <IconButton
                          aria-label="Edit details"
                          icon={<EditIcon />}
                          size="sm"
                          float="right"
                          onClick={() => startEditing(instance)}
                        />
                      </PopoverHeader>
                      <PopoverBody>
                        {editingInstance === instance ? (
                          <>
                            <Input
                              value={editDetails}
                              onChange={(e) => setEditDetails(e.target.value)}
                              size="sm"
                            />
                            <Button
                              mt="2"
                              colorScheme="blue"
                              size="sm"
                              onClick={() => submitEdit(instance)}
                            >
                              Submit
                            </Button>
                          </>
                        ) : (
                          <Text>{instance.details}</Text>
                        )}
                      </PopoverBody>
                    </PopoverContent>
                  </Popover>
                ))
              }
            </VStack>
          </>
        )}
      </Box>
    </Flex>
  );
}
