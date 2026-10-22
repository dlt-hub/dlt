'use strict'

const GITHUB_ACTIONS_BOT_LOGIN = 'github-actions[bot]'

const PULL_REQUEST_QUERY = `
  query($owner: String!, $repo: String!, $number: Int!) {
    repository(owner: $owner, name: $repo) {
      pullRequest(number: $number) {
        number
        state
        isDraft
        isCrossRepository
        authorAssociation
        author {
          login
        }
        closingIssuesReferences(first: 100) {
          nodes {
            number
            state
            repository {
              nameWithOwner
            }
            labels(first: 100) {
              nodes {
                name
              }
            }
          }
        }
      }
    }
  }
`

function isNotFound(error) {
  return (
    typeof error === 'object' &&
    error !== null &&
    'status' in error &&
    error.status === 404
  )
}

function createGitHubApi(github, repository) {
  const { owner, repo } = repository

  async function getPullRequest(number) {
    const result = await github.graphql(PULL_REQUEST_QUERY, { owner, repo, number })
    return result.repository.pullRequest
  }

  async function findGitHubActionsCommentContaining(number, marker) {
    const comments = await github.paginate(github.rest.issues.listComments, {
      owner,
      repo,
      issue_number: number,
      per_page: 100,
    })
    return comments.find(
      (comment) =>
        comment.user?.login === GITHUB_ACTIONS_BOT_LOGIN &&
        comment.body?.includes(marker),
    )
  }

  async function addLabel(number, name) {
    await github.rest.issues.addLabels({
      owner,
      repo,
      issue_number: number,
      labels: [name],
    })
  }

  async function removeLabel(number, name) {
    try {
      await github.rest.issues.removeLabel({
        owner,
        repo,
        issue_number: number,
        name,
      })
    } catch (error) {
      if (!isNotFound(error)) throw error
    }
  }

  async function createComment(number, body) {
    await github.rest.issues.createComment({
      owner,
      repo,
      issue_number: number,
      body,
    })
  }

  async function updateComment(commentId, body) {
    await github.rest.issues.updateComment({
      owner,
      repo,
      comment_id: commentId,
      body,
    })
  }

  async function ensureLabels(names) {
    for (const name of names) {
      try {
        await github.rest.issues.getLabel({ owner, repo, name })
      } catch (error) {
        if (!isNotFound(error)) throw error
        throw new Error(`Configured label '${name}' does not exist in ${owner}/${repo}`)
      }
    }
  }

  async function listPullRequestsWithLabel(label) {
    const items = await github.paginate(github.rest.issues.listForRepo, {
      owner,
      repo,
      state: 'open',
      labels: label,
      per_page: 100,
    })
    return items.filter((item) => item.pull_request)
  }

  async function closePullRequest(number) {
    await github.rest.pulls.update({
      owner,
      repo,
      pull_number: number,
      state: 'closed',
    })
  }

  return Object.freeze({
    addLabel,
    closePullRequest,
    createComment,
    ensureLabels,
    findGitHubActionsCommentContaining,
    getPullRequest,
    listPullRequestsWithLabel,
    removeLabel,
    updateComment,
  })
}

module.exports = { createGitHubApi }
