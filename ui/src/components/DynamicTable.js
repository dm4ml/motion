import Table from "@mui/joy/Table";

export default function DynamicTable({ tableData }) {
  const headers = Object.keys(tableData[0]);
  const data = tableData.map((row) => Object.values(row));

  return (
    <Table aria-label="dynamic table" sx={{ opacity: 0.7 }}>
      <thead>
        <tr>
          {headers.map((header) => (
            <th key={header}>{header}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {data.map((row, index) => (
          <tr key={index}>
            {row.map((cell, index) => (
              <td key={index}>{cell}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </Table>
  );
}
