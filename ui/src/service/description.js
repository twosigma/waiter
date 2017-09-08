import React, { Component } from 'react';
import { Table, TableBody, TableCell, TableHead, TableRow } from 'material-ui';


const ServiceDescription = ({ id, description }) => {
  return (
    <Table>
      <TableBody>
        {
          Object.keys(description).sort().map((k) => {
            const desc = description[k];
            return (
              <TableRow key={k}>
                <TableCell>{k}</TableCell>
                <TableCell>
                {
                  (typeof (desc) === 'object' && !Array.isArray(desc))
                  ? <ServiceDescription id={id} description={desc}/>
                  : desc
                }
                </TableCell>
              </TableRow>
            );
          })
        }
      </TableBody>
    </Table>
  );}

export default ServiceDescription;

