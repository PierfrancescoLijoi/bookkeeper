package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.commands.bookie.LocalConsistencyCheckCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import static org.junit.Assert.*;


public class InterleavedLedgerStorageIntegrationTestIT {

    private InterleavedLedgerStorage ledgerStorage;
    private ServerConfiguration serverConfig;
    private CliFlags cliFlags;
    private LocalConsistencyCheckCommand command;

    @Before
    public void setUp() throws IOException {
        // Crea una configurazione del server
        serverConfig = new ServerConfiguration();

        // Permetti l'uso dell'indirizzo di loopback
        serverConfig.setAllowLoopback(true);  // Consenti l'ascolto su loopback

        // Usa una vera implementazione di InterleavedLedgerStorage
        ledgerStorage = new InterleavedLedgerStorage();

        // Crea un'istanza di CliFlags
        cliFlags = new CliFlags();

        // Crea l'oggetto LocalConsistencyCheckCommand
        command = new LocalConsistencyCheckCommand();
    }

    @Test
    public void testLocalConsistencyCheckSuccess() throws IOException {
        // Configura il comportamento di localConsistencyCheck per non trovare errori
        List<LedgerStorage.DetectedInconsistency> detectedInconsistencies = ledgerStorage.localConsistencyCheck(Optional.empty());

        // Verifica che non ci siano incoerenze
        assertTrue(detectedInconsistencies.isEmpty());

        // Esegui il comando tramite il metodo apply (che a sua volta invoca check)
        boolean result = command.apply(serverConfig, cliFlags);

        // Verifica che il comando non abbia trovato errori

        assertEquals(true, result);
    }

}
