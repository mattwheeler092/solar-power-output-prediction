import React, { useState, useEffect } from "react";
import location_image from "./location_image.json";

export default function Cover(props) {
  const [date, setDate] = useState(new Date());

  useEffect(() => {
    const timerID = setInterval(() => tick(), 1000);

    return function cleanup() {
      clearInterval(timerID);
    };
  });

  function tick() {
    setDate(new Date());
  }

  return (
    <div className="left">
      <img src={location_image[props.location]} id="image" alt="" />
      <div className="image-overlay">
        <h1 id="location">{props.location}</h1>
        <div className="image-overlay-bottom">
          <div className="datetime">
            <p id="time">
              {date.toLocaleTimeString([], {
                hour: "numeric",
                minute: "numeric",
              })}
            </p>
            <p id="date">
              {date.toLocaleDateString([], {
                weekday: "long",
                day: "numeric",
                month: "long",
                year: "numeric",
              })}
            </p>
          </div>
          <div id="temperature">
            <h1>
              {props.data ? props.data.location.currentConditions.temp : "00.0"}
              °F
            </h1>
          </div>
        </div>
      </div>
    </div>
  );
}
