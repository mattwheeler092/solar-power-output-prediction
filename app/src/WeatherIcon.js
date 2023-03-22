import React from "react";

export default function WeatherIcon(props) {
  const renderWeatherIcon = (iconName) => {
    const icon = require(`../public/asset/weather_icons/${iconName}.svg`);
    return (
      <div className="weather_icon_container">
        <img id="weather_icon" src={icon} alt={iconName} />
      </div>
    );
  };

  return (
    <>
      {renderWeatherIcon(props.icon)}
      <h3>It's a {props.icon.replace("-day", "")} day!</h3>
    </>
  );
}
