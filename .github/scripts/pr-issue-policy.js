'use strict'

const { createGitHubApi } = require('./pr-issue-policy-github')

const POLICY_MARKER = '<!-- pr-issue-policy -->'
const PENDING_MARKER_PATTERN = /<!-- pr-issue-policy:pending-since=([^ ]+) -->/
const CLOSED_MARKER = '<!-- pr-issue-policy:closed -->'
const HOUR_IN_MS = 60 * 60 * 1000

function requireText(env, name) {
  const value = env[name]?.trim()
  if (!value) throw new Error(`${name} must not be empty`)
  return value
}

function requirePositiveNumber(env, name) {
  const value = Number(env[name])
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive number; received ${env[name]}`)
  }
  return value
}

function requirePositiveInteger(env, name) {
  const value = Number(env[name])
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer; received ${env[name]}`)
  }
  return value
}

function optionalPositiveInteger(env, name) {
  return env[name]?.trim() ? requirePositiveInteger(env, name) : null
}

function requireBoolean(env, name) {
  const value = env[name]?.trim().toLowerCase()
  if (value === 'true') return true
  if (value === 'false') return false
  throw new Error(`${name} must be true or false; received ${env[name]}`)
}

function csvSet(value, lowercase = false) {
  return new Set(
    (value ?? '')
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
      .map((item) => lowercase ? item.toLowerCase() : item.toUpperCase()),
  )
}

function loadConfig(env) {
  const pendingLabel = requireText(env, 'PENDING_PR_LABEL')
  const verifiedLabel = requireText(env, 'VERIFIED_PR_LABEL')

  if (pendingLabel.toLowerCase() === verifiedLabel.toLowerCase()) {
    throw new Error('PENDING_PR_LABEL and VERIFIED_PR_LABEL must be different labels')
  }

  return Object.freeze({
    authorizingIssueLabel: requireText(env, 'AUTHORIZING_ISSUE_LABEL'),
    pendingLabel,
    verifiedLabel,
    graceHours: requirePositiveNumber(env, 'GRACE_HOURS'),
    permissionBypass: requireBoolean(env, 'PERMISSION_BYPASS'),
    exemptSameRepository: requireBoolean(env, 'EXEMPT_SAME_REPOSITORY'),
    exemptAssociations: csvSet(env.EXEMPT_ASSOCIATIONS),
    exemptLogins: csvSet(env.EXEMPT_LOGINS, true),
  })
}

function findQualifyingIssue(pr, config, repositoryName) {
  return pr.closingIssuesReferences.nodes.find(
    (issue) =>
      issue.state === 'OPEN' &&
      issue.repository.nameWithOwner.toLowerCase() === repositoryName &&
      issue.labels.nodes.some(
        (label) => label.name.toLowerCase() === config.authorizingIssueLabel.toLowerCase(),
      ),
  )
}

function classifyPullRequest(pr, config, repositoryName) {
  if (!pr) return Object.freeze({ status: 'missing' })
  if (pr.state !== 'OPEN') return Object.freeze({ status: 'closed', pr })
  if (pr.isDraft) return Object.freeze({ status: 'draft', pr })

  if (config.exemptSameRepository && !pr.isCrossRepository) {
    return Object.freeze({
      status: 'exempt',
      pr,
      exemption: Object.freeze({
        type: 'same-repository',
        value: repositoryName,
      }),
    })
  }

  if (
    config.permissionBypass &&
    config.exemptAssociations.has(pr.authorAssociation)
  ) {
    return Object.freeze({
      status: 'exempt',
      pr,
      exemption: Object.freeze({
        type: 'association',
        value: pr.authorAssociation,
      }),
    })
  }

  const login = pr.author?.login?.toLowerCase()
  if (login && config.exemptLogins.has(login)) {
    return Object.freeze({
      status: 'exempt',
      pr,
      exemption: Object.freeze({ type: 'login', value: login }),
    })
  }

  const issue = findQualifyingIssue(pr, config, repositoryName)
  if (issue) return Object.freeze({ status: 'verified', pr, issue })
  return Object.freeze({ status: 'unverified', pr })
}

function pendingBody(config, since) {
  const days = config.graceHours / 24
  const duration = Number.isInteger(days) ? `${days} days` : `${config.graceHours} hours`
  return `${POLICY_MARKER}
<!-- pr-issue-policy:pending-since=${since} -->
Thanks for contributing to dlt!

We ask new contributors to open pull requests only for issues labeled \`${config.authorizingIssueLabel}\`. To link the issue you’re addressing, add \`Closes #123\` to the pull request description, replacing \`123\` with the issue number. You can also ask a maintainer to link the issue through GitHub’s Development sidebar.

We’ll periodically check whether the linked issue is still open and has the required label. If this pull request remains unverified for ${duration}, it may be closed automatically.`
}

function resolvedBody(config, issueNumber) {
  return `${POLICY_MARKER}
This pull request now references eligible issue #${issueNumber}. The \`${config.pendingLabel}\` classification has been replaced with \`${config.verifiedLabel}\`.`
}

function closedBody() {
  return `${CLOSED_MARKER}
This pull request has been automatically closed because it remained unverified for too long. It may be reopened after it references an eligible issue.`
}

async function setClassificationLabels(api, config, number, classification) {
  if (classification === 'verified') {
    await api.removeLabel(number, config.pendingLabel)
    await api.addLabel(number, config.verifiedLabel)
    return
  }
  if (classification === 'unverified') {
    await api.removeLabel(number, config.verifiedLabel)
    await api.addLabel(number, config.pendingLabel)
    return
  }
  if (classification === 'exempt') {
    await api.removeLabel(number, config.pendingLabel)
    await api.removeLabel(number, config.verifiedLabel)
    return
  }
  throw new Error(`Unsupported label classification '${classification}'`)
}

async function markUnverified(api, config, number, apply, now) {
  const comment = await api.findGitHubActionsCommentContaining(number, POLICY_MARKER)
  const existingSince = comment?.body?.match(PENDING_MARKER_PATTERN)?.[1]
  if (existingSince) {
    if (apply) await setClassificationLabels(api, config, number, 'unverified')
    return existingSince
  }

  const since = new Date(now()).toISOString()
  if (!apply) return since

  await setClassificationLabels(api, config, number, 'unverified')
  if (comment) {
    await api.updateComment(comment.id, pendingBody(config, since))
  } else {
    await api.createComment(number, pendingBody(config, since))
  }
  return since
}

async function markVerified(api, config, number, issueNumber, apply) {
  if (!apply) return
  await setClassificationLabels(api, config, number, 'verified')
  const comment = await api.findGitHubActionsCommentContaining(number, POLICY_MARKER)
  if (!comment?.body?.match(PENDING_MARKER_PATTERN)) return
  await api.updateComment(comment.id, resolvedBody(config, issueNumber))
}

function exemptionReason(exemption) {
  if (exemption.type === 'same-repository') {
    return `source branch belongs to ${exemption.value}`
  }
  if (exemption.type === 'association') {
    return `author association ${exemption.value} is configured for permission bypass`
  }
  return `author @${exemption.value} is listed in PR_EXEMPT_LOGINS`
}

function skippedReason(status) {
  if (status === 'missing') return 'the pull request was not returned by GitHub'
  if (status === 'closed') return 'the pull request is not open'
  if (status === 'draft') return 'draft pull requests are outside the policy'
  return `unsupported classification '${status}'`
}

async function reconcilePullRequest(dependencies, number, apply) {
  const { api, config, core, now, repositoryName } = dependencies
  const pr = await api.getPullRequest(number)
  const result = classifyPullRequest(pr, config, repositoryName)

  if (result.status === 'verified') {
    await markVerified(api, config, number, result.issue.number, apply)
    core.info(
      `PR #${number} is verified: linked issue #${result.issue.number} is open and labeled '${config.authorizingIssueLabel}'`,
    )
    return result
  }

  if (result.status === 'exempt') {
    if (apply) await setClassificationLabels(api, config, number, 'exempt')
    core.info(`PR #${number} is exempt: ${exemptionReason(result.exemption)}`)
    return result
  }

  if (result.status !== 'unverified') {
    core.info(`Skipping PR #${number}: ${skippedReason(result.status)}`)
    return result
  }

  const pendingSince = await markUnverified(api, config, number, apply, now)
  core.info(
    `PR #${number} is unverified since ${pendingSince}: no linked open issue in ${repositoryName} has label '${config.authorizingIssueLabel}'`,
  )
  return Object.freeze({ ...result, pendingSince })
}

function expirationTime(pendingSince, graceHours) {
  const timestamp = Date.parse(pendingSince)
  if (Number.isNaN(timestamp)) {
    throw new Error(`Invalid pending timestamp: ${pendingSince}`)
  }
  return timestamp + graceHours * HOUR_IN_MS
}

function isExpired(pendingSince, graceHours, now) {
  return now() >= expirationTime(pendingSince, graceHours)
}

async function sweep(dependencies, { dryRun, maxClose }) {
  const { api, config, core, now } = dependencies
  const candidates = await api.listPullRequestsWithLabel(config.pendingLabel)
  core.info(
    `Starting ${dryRun ? 'dry-run ' : ''}sweep: found ${candidates.length} open PRs labeled '${config.pendingLabel}'`,
  )

  let selected = 0
  for (const candidate of candidates) {
    const result = await reconcilePullRequest(dependencies, candidate.number, !dryRun)
    if (result.status !== 'unverified') continue
    if (!isExpired(result.pendingSince, config.graceHours, now)) {
      const closesAfter = new Date(
        expirationTime(result.pendingSince, config.graceHours),
      ).toISOString()
      core.info(`PR #${candidate.number} remains in its grace period until ${closesAfter}`)
      continue
    }
    if (selected >= maxClose) {
      core.warning(`Reached max-close limit of ${maxClose}`)
      break
    }

    // Re-read immediately before closing to avoid acting on stale sweep data.
    const finalPr = await api.getPullRequest(candidate.number)
    const finalResult = classifyPullRequest(
      finalPr,
      config,
      dependencies.repositoryName,
    )
    if (finalResult.status !== 'unverified') {
      core.info(`PR #${candidate.number} changed classification before closure; skipping`)
      continue
    }

    selected++
    if (dryRun) {
      core.warning(`[dry-run] Would close PR #${candidate.number}`)
      continue
    }

    await api.createComment(candidate.number, closedBody())
    await api.closePullRequest(candidate.number)
    core.info(`Closed PR #${candidate.number}`)
  }

  core.info(`${dryRun ? 'Selected' : 'Closed'} ${selected} PRs`)
}

async function run({ github, context, core, env = process.env, now = Date.now }) {
  const config = loadConfig(env)
  const repositoryName = `${context.repo.owner}/${context.repo.repo}`.toLowerCase()
  const api = createGitHubApi(github, context.repo)
  const dependencies = Object.freeze({ api, config, core, now, repositoryName })

  core.info(
    `Same-repository bypass ${config.exemptSameRepository ? 'enabled' : 'disabled'}`,
  )

  if (config.permissionBypass) {
    const associations = [...config.exemptAssociations].join(', ') || '(none)'
    core.info(`Permission bypass enabled for author associations: ${associations}`)
  } else {
    core.info('Permission bypass disabled; author associations are evaluated normally')
  }

  if (config.exemptLogins.size > 0) {
    core.info(`Explicit login exemptions: ${[...config.exemptLogins].map((login) => `@${login}`).join(', ')}`)
  }

  await api.ensureLabels([
    config.authorizingIssueLabel,
    config.pendingLabel,
    config.verifiedLabel,
  ])

  switch (context.eventName) {
    case 'pull_request_target': {
      const number = context.payload.pull_request.number
      core.info(`Handling pull_request_target '${context.payload.action}' for PR #${number}`)
      return reconcilePullRequest(dependencies, number, true)
    }
    case 'workflow_dispatch': {
      const dryRun = requireBoolean(env, 'DRY_RUN')
      const prNumber = optionalPositiveInteger(env, 'DISPATCH_PR_NUMBER')
      if (prNumber !== null) {
        core.info(
          `Handling ${dryRun ? 'dry-run ' : ''}manual reclassification for PR #${prNumber}`,
        )
        return reconcilePullRequest(dependencies, prNumber, !dryRun)
      }
      return sweep(dependencies, {
        dryRun,
        maxClose: requirePositiveInteger(env, 'DISPATCH_MAX_CLOSE'),
      })
    }
    case 'schedule':
      return sweep(dependencies, {
        dryRun: false,
        maxClose: requirePositiveInteger(env, 'DEFAULT_MAX_CLOSE'),
      })
    default:
      throw new Error(`Unsupported workflow event: ${context.eventName}`)
  }
}

module.exports = {
  classifyPullRequest,
  loadConfig,
  run,
}
