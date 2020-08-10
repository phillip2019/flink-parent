package com.aikosolar.bigdata.flink.table;

import java.util.Set;
import java.util.TreeSet;

/**
 * @author carlc
 */
public class ScriptManager {

    private Set<Script> scripts = new TreeSet<>();
    private Set<Setting> settings = new TreeSet<>();

    private ScriptManager() {
    }

    public static ScriptManager me() {
        return new ScriptManager();
    }

    public ScriptManager addScript(Script script) {
        this.scripts.add(script);
        return this;
    }

    public ScriptManager addSetting(Setting setting) {
        this.settings.add(setting);
        return this;
    }

    public Set<Script> getScripts() {
        return this.scripts;
    }

    public Set<Setting> getSettings() {
        return this.settings;
    }
}