import React from "react";
import WeatherIcon from "./WeatherIcon";
import CurConditions from "./CurConditions";
import AnnualSaving from "./AnnualSaving";

export default function Details(props) {
  return (
    <div className="right">
      {" "}
      {props.data ? (
        <>
          <div className="right_top">
            <WeatherIcon icon={props.data.location.currentConditions.icon} />{" "}
          </div>{" "}
          <div className="right_mid">
            <div className="weather_details">
              <CurConditions data={props.data} />{" "}
            </div>{" "}
          </div>{" "}
        </>
      ) : (
        <div> Loading... </div>
      )}{" "}
      <div className="right_bottom">
        <AnnualSaving
          location={props.location}
          numPanels={props.numPanels}
          efficiency={props.efficiency}
        />{" "}
      </div>{" "}
    </div>
  );
}
