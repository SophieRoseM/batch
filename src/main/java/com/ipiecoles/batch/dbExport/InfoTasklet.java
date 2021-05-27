package com.ipiecoles.batch.dbExport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class InfoTasklet implements Tasklet {

    Logger logger = LoggerFactory.getLogger(this.getClass());

    private String message = null;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        System.out.println("Début du traitement de la bdd vers le fichier txt");
        // Transmettre le message à la step exportCommune
        message = "Le traitement est fini.";
        return RepeatStatus.FINISHED;
    }

    @BeforeStep
    public void beforeStep(StepExecution sExec) throws Exception {
        //Avant l'exécution de la Step
        logger.info("Tasklet OK");
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution sExec) throws Exception {
        //Une fois la Step exécutée
        sExec.getJobExecution().getExecutionContext().put("MSG", message);
        logger.info("Fin de l'export de la table Commune");
        logger.info(sExec.getSummary());
        return ExitStatus.COMPLETED;
    }

}
