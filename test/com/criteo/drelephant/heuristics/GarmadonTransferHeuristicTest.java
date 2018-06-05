package com.criteo.drelephant.heuristics;

import com.avaje.ebean.Ebean;
import com.linkedin.drelephant.analysis.Severity;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.junit.Before;
import org.junit.Test;
import play.test.WithApplication;

import java.util.List;

import static common.DBTestUtil.initDB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.inMemoryDatabase;

public class GarmadonTransferHeuristicTest extends WithApplication {

    private void populateTestData() {
        try {
            initDB();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() {
        start(fakeApplication(inMemoryDatabase()));
        populateTestData();

        AppResult appResult = Ebean.find(AppResult.class, "application_1458194917883_1453361");
        assertEquals("Severity should be initialized at NONE", Severity.NONE, appResult.severity);

        AppHeuristicResult appHeuristicResults = Ebean.find(AppHeuristicResult.class)
                .where()
                .eq("yarn_app_result_id", appResult.id)
                .eq("heuristic_name", "moderate")
                .findUnique();

        assertNull("A moderate heuristic must not exist", appHeuristicResults);
    }

    @Test
    public void shouldPushGarmadonHeuristicsToApp() {
        GarmadonTransferHeuristic.transfer();
        AppResult appResult = Ebean.find(AppResult.class, "application_1458194917883_1453361");
        assertEquals("Severity should have been updated to CRITICAL", Severity.CRITICAL, appResult.severity);

        AppHeuristicResult appHeuristicResults = Ebean.find(AppHeuristicResult.class)
                .where()
                .eq("yarn_app_result_id", appResult.id)
                .eq("heuristic_name", "moderate")
                .findUnique();

        assertNotNull("A moderate heuristic must have been created", appHeuristicResults);

        List<AppHeuristicResultDetails> appHeuristicResultDetails = Ebean.find(AppHeuristicResultDetails.class)
                .where().eq("yarn_app_heuristic_result_id", appHeuristicResults.id)
                .findList();
        assertEquals("Some detail heuristics must have been added to the moderate heuristic", 3, appHeuristicResultDetails.size());
    }

    @Test
    public void shouldNotPushGarmadonHeuristicsToAppAsItIsNotReady() {
        GarmadonTransferHeuristic.transfer();
        AppResult appResult = Ebean.find(AppResult.class, "application_1458194917883_1453362");

        AppHeuristicResult appHeuristicResults = Ebean.find(AppHeuristicResult.class)
                .where()
                .eq("yarn_app_result_id", appResult.id)
                .eq("heuristic_name", "moderate")
                .findUnique();

        assertNull("A moderate heuristic must not exist", appHeuristicResults);
    }
}
