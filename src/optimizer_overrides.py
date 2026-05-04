from config.access import require_config_value


def optimizer_overrides(overrides_list, config, pside):
    if not overrides_list:
        # 没有要应用的覆盖
        return config

    for override in overrides_list:
        if override == "lossless_close_trailing":

            # 无损平仓逻辑
            threshold = require_config_value(config, f"bot.{pside}.close_trailing_threshold_pct")
            retracement = require_config_value(config, f"bot.{pside}.close_trailing_retracement_pct")
            config["bot"][pside]["close_trailing_threshold_pct"] = max(threshold, retracement)

        elif override == "forward_tp_grid":
            close_grid_markup_start = require_config_value(
                config, f"bot.{pside}.close_grid_markup_start"
            )
            close_grid_markup_end = require_config_value(config, f"bot.{pside}.close_grid_markup_end")

            config["bot"][pside]["close_grid_markup_start"] = min(
                close_grid_markup_start, close_grid_markup_end
            )
            config["bot"][pside]["close_grid_markup_end"] = max(
                close_grid_markup_start, close_grid_markup_end
            )

        elif override == "backward_tp_grid":
            close_grid_markup_start = require_config_value(
                config, f"bot.{pside}.close_grid_markup_start"
            )
            close_grid_markup_end = require_config_value(config, f"bot.{pside}.close_grid_markup_end")

            config["bot"][pside]["close_grid_markup_start"] = max(
                close_grid_markup_start, close_grid_markup_end
            )
            config["bot"][pside]["close_grid_markup_end"] = min(
                close_grid_markup_start, close_grid_markup_end
            )

        elif override == "example":
            # 覆盖 'example' 的逻辑
            pass

        else:
            print(f"Unknown override: {override}")
            return config

    return config
