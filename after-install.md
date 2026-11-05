# Hermes Mobile — notifications

`hermes-mobile-notify` est installé. Il reste trois étapes.

1. **Activer le plugin** si l'installation n'a pas utilisé `--enable` :

   ```sh
   hermes plugins enable hermes-mobile-notify
   ```

2. **Enregistrer la clé Firebase** : Hermes Desktop → **Settings → Plugins** →
   engrenage de la ligne **hermes-mobile-notify** (Hermes Mobile) → champ
   **Compte de service Firebase** → coller le JSON du compte de service (ou le
   même JSON en base64) → **Save**.

   Sans Desktop : déposer le JSON dans
   `HERMES_HOME/hermes-mobile/service-account.json`. Si ce fichier existe déjà,
   cette étape est facultative.

3. **Redémarrer Hermes** (add-on Home Assistant : **Redémarrer**) pour charger
   le hook.

Vérification : envoyer un message depuis l'app, quitter l'app, puis attendre la
fin d'un tour. La notification doit arriver. En cas de silence, consulter
`HERMES_HOME/plugins/hermes-mobile-notify/session-end.jsonl` (`no_token`,
`no_key`, `key_invalid`…) et `logs/agent.log`.

Mails : aucun réglage. Une fois le plugin chargé, chaque `gmail send` ou
`gmail reply` du skill Google Workspace demande une validation par carte dans
l'app.

Brouillons : installer aussi `hermes plugins install arsiktech/hermes-draft-cards
--enable`, puis redémarrer Hermes. Les nouvelles conversations affichent les
brouillons en carte avec Modifier et Envoyer.

Ne jamais copier la clé Firebase dans le dépôt ni dans le dossier du plugin.
