# Plugin Hermes `hermes-mobile-notify`

Envoie une notification FCM avec le **sujet de la dernière demande** dans le
titre et un **extrait de la réponse** dans le corps (100 et 600 octets UTF-8).
Le Markdown est allégé ; les blocs de code et de réflexion sont retirés.
Les échecs et arrêts gardent leur libellé explicite. Si aucun aperçu ne peut
être lu, les libellés génériques restent disponibles. `data` ne porte que
`outcome` et `sessionId`. Le contenu est lu dans le `state.db` du profil en
lecture seule, dans le fil d'envoi, et ne figure jamais dans les journaux.
Les aperçus transitent par FCM et sont visibles dans les notifications Android,
y compris sur l'écran verrouillé selon les réglages du téléphone. L'envoi
part dans un fil en arrière-plan : le tour n'attend jamais le réseau, et toute
erreur (jeton absent, clé absente, FCM en échec) devient une ligne de journal.
Seuls les tours `platform: tui` (l'app) et les fins de routine `platform: cron`
(libellé « Routine terminée / en échec ») notifient ; les autres (sous-agents
`subagent`…) sont journalisés `skipped`, `reason: platform`.

- Commande `/hermes-mobile-token <jeton>` : mémorise le jeton FCM dans
  `HERMES_HOME/hermes-mobile/fcm-token`. L'app l'appelle elle-même
  (`command.dispatch`) avant chaque envoi de message, sans jamais bloquer
  l'envoi. Un seul jeton par profil : le dernier téléphone qui l'envoie reçoit
  les notifications du profil.
- Erreurs FCM : un seul nouvel essai, environ 2 s plus tard, sur erreur réseau
  ou HTTP 5xx ; tout refus 4xx échoue sans nouvel essai. Un jeton refusé
  (`UNREGISTERED`) est oublié (fichier `fcm-token` supprimé) jusqu'au prochain
  message envoyé depuis l'app. Pas de file durable : si Hermes s'arrête en plein
  tour, la notification est perdue.

Auto-vérification :
`python __init__.py`.

- Commande `/hermes-mobile-quotas` : renvoie en JSON les quotas 5h et 7 jours
  des comptes Claude et Codex, pour la carte de l'accueil de l'app. Elle passe par
  `agent.account_usage` de Hermes (le même code que `/usage`) : le jeton reste
  sur le serveur, seuls les pourcentages sortent. Lecture gardée 60 s. Un
  compte illisible renvoie `{}` et l'app affiche `—`. Ces chiffres viennent de
  routes non documentées d'OpenAI et d'Anthropic, qui peuvent changer.

## Installation

Dans le terminal Hermes du profil à notifier :

```sh
hermes plugins install alanmlcrt/hermes-mobile-notify --enable
```


- `--enable` active le hook sans question. Sans cette option, répondre `y` à
  `Enable 'hermes-mobile-notify' now?`, ou lancer ensuite
  `hermes plugins enable hermes-mobile-notify`.
- Le plugin s'installe **par profil**. Lister les profils avec `hermes profile list`,
  puis pour chacun : `hermes -p <profil> plugins install alanmlcrt/hermes-mobile-notify --enable`.
- Si une copie manuelle existe déjà dans `HERMES_HOME/plugins/`, ajouter
  `--force` (ou lancer `hermes plugins remove hermes-mobile-notify` avant).
- Aucun fichier de Hermes n'est à copier ni à modifier à la main.

## Clé Firebase

Le plugin lit d'abord le réglage secret du plugin, puis, en secours,
`HERMES_HOME/hermes-mobile/service-account.json`. Si ce fichier existe déjà
(installation antérieure), aucun autre réglage n'est nécessaire.

### Avec Hermes Desktop (recommandé)

1. Ouvrir Hermes Desktop connecté au backend du profil.
2. **Settings → Plugins**, puis l'engrenage de la ligne
   **hermes-mobile-notify** (Hermes Mobile).
3. Champ **Compte de service Firebase** : coller le JSON complet du compte de
   service, ou le même JSON encodé en base64, puis **Save**.

Le secret est rangé dans le `.env` du profil ; il n'apparaît ni dans
`config.yaml`, ni dans le dossier du plugin, ni dans le dépôt.

### Sans Desktop (installation headless)

Déposer le JSON dans `HERMES_HOME/hermes-mobile/service-account.json` (créer le
dossier `hermes-mobile/` si besoin). Ne jamais mettre cette clé dans le dossier
du plugin ni dans le dépôt.

La clé est celle d'un compte de service du projet Firebase personnel, avec le
rôle `roles/firebasecloudmessaging.admin` : JSON généré depuis la console
Firebase (Paramètres du projet → Comptes de service).

## Redémarrer

Le gateway découvre les plugins à son démarrage : redémarrer Hermes après
l'installation. Un changement de clé est relu au tour suivant ; redémarrer
seulement s'il n'est pas pris en compte :

- add-on Home Assistant : **Redémarrer** l'add-on ;
- sinon : quitter puis relancer le gateway/Desktop.

## Vérifier

1. `hermes plugins list` montre `hermes-mobile-notify` **enabled**.
2. Dans une conversation, `/hermes-mobile-token` sans argument répond par
   l'usage ; `logs/agent.log` ne contient aucune erreur de chargement.
3. Envoyer un message depuis l'app (la permission de notification doit être
   accordée) : `HERMES_HOME/hermes-mobile/fcm-token` existe et le journal porte
   une ligne `event: token`.
4. Quitter l'app, puis attendre la fin d'un tour : la notification arrive et le
   journal porte `action: sent`.

## Mise à jour

```sh
hermes plugins update hermes-mobile-notify
```

Si la mise à jour échoue (ancienne copie manuelle par exemple), réinstaller :

```sh
hermes plugins install alanmlcrt/hermes-mobile-notify --force --enable
```

Puis redémarrer Hermes. Version actuelle : **1.2.0** (quotas de compte pour l'accueil de
l'app). Le sujet et l'extrait de réponse datent de la 1.1.0 ; mettre aussi l'app à
jour pour l'affichage au premier plan. Le réglage
secret et la lecture du JSON en base64 datent de la 1.0.3.

## Journal

`HERMES_HOME/plugins/hermes-mobile-notify/session-end.jsonl`, une ligne par fin
de tour (`action: sent | skipped | failed`, `status` HTTP, `reason`,
`attempts`, `duration_ms`, `outcome`, métadonnées du tour) et une ligne
`event: token` par enregistrement du jeton. Du jeton, seuls les 8 derniers
caractères (`token_tail`) sont écrits ; `agent.log` reçoit une ligne `INFO`
`hermes-mobile-notify …` par tour.

Raisons courantes : `platform` (tour hors app), `session_close` (fermeture de
session sans `turn_id`, par exemple session détachée récoltée après que l'app a
été quittée : ce n'est pas une fin de tour), `no_token` (l'app n'a pas encore
envoyé de jeton, ou il a été refusé par FCM), `no_key` (clé absente),
`key_invalid`, `no_session_id`, `UNREGISTERED`, un autre code FCM tel quel
(`INVALID_ARGUMENT`, `QUOTA_EXCEEDED`, `UNAVAILABLE`…), `fcm_http` (refus sans
code), ou le nom de l'erreur réseau.

## Désinstallation

```sh
hermes plugins remove hermes-mobile-notify
```

Le retrait enlève aussi le plugin de `plugins.enabled`/`plugins.disabled` et de
ses réglages. Supprimer ensuite, si souhaité, `HERMES_HOME/hermes-mobile/`
(jeton et clé de secours), puis redémarrer Hermes.
