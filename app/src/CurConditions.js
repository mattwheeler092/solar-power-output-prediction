import React from "react";
import { BsFillCloudRainFill, BsFillCloudFill } from "react-icons/bs";
import { GiWaterDrop } from "react-icons/gi";
import { MdVisibility } from "react-icons/md";

export default function CurConditions(props) {
  const renderIcon = (item) => {
    switch (item) {
      case "precip":
        return <BsFillCloudRainFill />;
      case "humidity":
        return <GiWaterDrop />;
      case "cloudcover":
        return <BsFillCloudFill />;
      case "visibility":
        return <MdVisibility />;
      default:
        return null;
    }
  };

  const keyToName = {
    precip: "Precipitation",
    humidity: "Humidity",
    cloudcover: "Cloud Cover",
    visibility: "Visibility",
  };

  const keyToUnit = {
    precip: "in",
    humidity: "%",
    cloudcover: "%",
    visibility: "mi",
  };

  return ["precip", "humidity", "cloudcover", "visibility"].map((item, i) => {
    return (
      <div className="weather_details_container">
        <div className="weather_details_icon">{renderIcon(item)}</div>
        <span className="weather_details_name">{keyToName[item]}</span>
        <div>
          {props.data.location.currentConditions[item]} {keyToUnit[item]}
        </div>
      </div>
    );
  });
}
