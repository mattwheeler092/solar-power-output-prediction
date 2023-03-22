import React, { useState, useEffect } from "react";
import "./Board.css";
import Cover from "./Cover";
import Details from "./Details";

export default function Board(props) {
  const [data, setData] = React.useState(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState(null);
  const API_KEY = "VH42VVTLSFV9W4LND9NV4W9VX";

  // API call to get current weather data
  useEffect(() => {
    const getData = async () => {
      try {
        const response = await fetch(
          `https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/forecast?aggregateHours=24&contentType=json&unitGroup=us&locationMode=single&key=${API_KEY}&locations=${props.location.replace(
            " ",
            "%20"
          )}`
        );
        if (!response.ok) {
          throw new Error(
            `This is an HTTP error: The status is ${response.status}`
          );
        }
        let actualData = await response.json();
        setData(actualData);
        setError(null);
      } catch (err) {
        setError(err.message);
        setData(null);
      } finally {
        setLoading(false);
      }
    };
    getData();
  }, [props.location]);

  return (
    <div className="Board">
      <Cover location={props.location} data={data} />
      <Details
        data={data}
        location={props.location}
        numPanels={props.numPanels}
        efficiency={props.efficiency}
      />
    </div>
  );
}
