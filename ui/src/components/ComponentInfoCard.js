import React from "react";
import { Card, Typography } from "@mui/joy";
import {
  Tracker,
  BarList,
  Title,
  Flex,
  Text,
  Bold,
  Metric,
  BadgeDelta,
  Card as TremorCard,
  Grid,
} from "@tremor/react";

const ComponentInfoCard = ({
  componentName,
  numInstances,
  statusCounts,
  statusChanges,
  fractionUptime,
  flowCounts,
  statusBars,
}) => {
  return (
    <Card variant="plain">
      <Typography level="h4">{componentName}</Typography>
      <Grid numItemsLg={3} className="gap-6">
        <TremorCard>
          <Flex alignItems="start">
            <div className="truncate">
              <Text>Success (Last 24 Hr)</Text>
              <Metric className="truncate">{statusCounts["success"]}</Metric>
            </div>
            <BadgeDelta deltaType={statusChanges?.success?.deltaType}>
              {statusChanges?.success?.value}
            </BadgeDelta>
          </Flex>
        </TremorCard>
        <TremorCard>
          <Flex alignItems="start">
            <div className="truncate">
              <Text>Failure (Last 24 Hr)</Text>
              <Metric className="truncate">{statusCounts["failure"]}</Metric>
            </div>
            <BadgeDelta deltaType={statusChanges?.failure?.deltaType}>
              {statusChanges?.failure?.value}
            </BadgeDelta>
          </Flex>
        </TremorCard>
        <TremorCard>
          <Flex alignItems="start">
            <div className="truncate">
              <Text>Instances</Text>
              <Metric className="truncate">{numInstances}</Metric>
            </div>
          </Flex>
        </TremorCard>
      </Grid>
      <TremorCard>
        <Title>Status</Title>
        <Flex justifyContent="end">
          <Text>Uptime {fractionUptime}%</Text>
        </Flex>
        <Tracker data={statusBars} className="mt-4" />
        <Flex>
          <Text>24 hours ago</Text>
          <Text>Now</Text>
        </Flex>
      </TremorCard>
      <TremorCard>
        <Title>Distribution</Title>
        <Text>Last 24 Hr</Text>
        <Flex className="mt-4">
          <Text>Flow</Text>
          <Text># Runs</Text>
        </Flex>
        <BarList data={flowCounts} className="mt-2" />
      </TremorCard>
    </Card>
  );
};

export default ComponentInfoCard;
