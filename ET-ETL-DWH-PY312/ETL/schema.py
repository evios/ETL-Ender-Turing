from sqlalchemy import Column, Integer, Boolean, Float, JSON, DateTime, UniqueConstraint, Unicode, UnicodeText, ForeignKey
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

# Define a base class using the declarative base
Base = declarative_base()


# TODO Provide/Write Table detailed description
# Define the tables
# --- Base Dictionaries - stable, rarely changed dicts for Agents/ Users/ Tags, etc ---
class Agent(Base):  # Base Dictionary
    """
    The Agent class represents the 'agents' table in the database.
    It contains details about each agent such as name, phone number,
    whether they are active, and the date they were deactivated.
    """
    __tablename__ = 'agents'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    phone_number = Column(Unicode)
    is_active = Column(Boolean)
    deactivated_at = Column(DateTime)  # '2024-06-26T10:15:44.620796'
    # phone_number_aliases  # ['6812'] - not interested in this data for DWH
    # user - will be mirrored in User data
    # reactions - not interested in this data for DWH
    __table_args__ = (
        UniqueConstraint('id', name='_agents_uc'),
    )

    agent = relationship("AgentGroupAssociation", back_populates="agent")
    user = relationship("User", back_populates="agent")
    session = relationship("Session", back_populates="agent")


class Scorecard(Base):  # Base Dictionary
    """
    The Scorecard class represents the 'scorecards' table in the database.
    Scorecard itself used for quality evaluation of sessions purpose.
    It contains details about each scorecard such as name, type, behavior on NA click in UI,
        and various flags indicating its properties.
    """
    __tablename__ = 'scorecards'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    type = Column(Unicode)
    na_behavior = Column(Unicode)
    count_critical_scores = Column(Boolean)
    is_automated = Column(Boolean)
    is_protected = Column(Boolean)
    is_default = Column(Boolean)
    is_archived = Column(Boolean)
    # team_ids  # [3, 5, 6, 10, 25] - not interested in this data for DWH
    __table_args__ = (
        UniqueConstraint('id', name='_scorecards_uc'),
    )

    # Relationship
    group = relationship("Group", back_populates="scorecard")
    category = relationship("ScorecardCategory", back_populates="scorecard")
    point = relationship("ScorecardPoint", back_populates="scorecard")
    session_score = relationship("SessionScore", back_populates="scorecard")


class Group(Base):  # Base Dictionary
    """
    The Group class represents the 'groups' table in the database.
    It contains details about each group such as name, associated scorecard,
        and whether it is the default group.
    """
    __tablename__ = 'groups'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    scorecard_id = Column(Integer, ForeignKey('scorecards.id'))
    is_default = Column(Boolean)
    # additional_scorecards:  - not interested in this data for DWH
    __table_args__ = (
        UniqueConstraint('id', name='_agent_groups_uc'),
    )

    # Relationships
    scorecard = relationship("Scorecard", back_populates="group")
    group = relationship("AgentGroupAssociation", back_populates="group")
    user = relationship("User", back_populates="group")
    tag = relationship("Tag", back_populates="group")
    session = relationship("Session", back_populates="group")


class AgentGroupAssociation(Base):
    """
    The AgentGroupAssociation class represents the 'agent_group_associations' table in the database.
    It contains the associations between agents and groups, including the start date of the association.
    When Agent moves from one team to another, new association will be created in this table.
    """
    __tablename__ = 'agent_group_associations'
    id = Column(Integer, primary_key=True, autoincrement=True)
    group_id = Column(Integer, ForeignKey('groups.id'))
    agent_id = Column(Integer, ForeignKey('agents.id'))
    start_dt = Column(DateTime)  # '0001-01-01T00:00:00'
    __table_args__ = (
        UniqueConstraint('group_id', 'agent_id', 'start_dt', name='_agent_group_associations_uc'),
    )

    agent = relationship("Agent", back_populates="agent")
    group = relationship("Group", back_populates="group")


class User(Base):  # Base Dictionary
    """
    The User class represents the 'users' table in the database.
    It contains details about each user such as email, activity status, superuser status, full name,
    associated agent and group, language, UUID, and invitation expiration date.
    """
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    email = Column(Unicode)
    is_active = Column(Boolean)
    is_superuser = Column(Boolean)
    full_name = Column(Unicode)
    agent_id = Column(Integer, ForeignKey('agents.id'))
    agent_group_id = Column(Integer, ForeignKey('groups.id'))
    language = Column(Unicode)
    uuid = Column(UNIQUEIDENTIFIER)  # 39094809-18df-4bd0-aa6e-9aced976d823
    invite_expires = Column(DateTime)  # '2024-06-26T10:15:44.620796'
    # role_ids  # [2] - not interested in this data for DWH
    # permissions - not interested in this data for DWH
    __table_args__ = (
        UniqueConstraint('id', name='_users_uc'),
    )

    agent = relationship("Agent", back_populates="user")
    group = relationship("Group", back_populates="user")
    session_reviewer = relationship("SessionReviewer", back_populates="reviewer")
    session_score = relationship("SessionScore", back_populates="reviewer")
    session_comment = relationship("SessionComment", back_populates="comment_author")


class Category(Base):  # Base Dictionary
    """
    The Category class represents the 'categories' table in the database.
    ! This is related to Topics of conversation, not Categories in Scorecards
    It contains details about each category such as name, filter data, position, and timestamps
        for creation and update.
    updated_at value should be used for incremental analytics update for already synced sessions.
    """
    __tablename__ = 'categories'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    filter_data = Column(Unicode)  # '&&[tags,||and|2738|or]'
    position = Column(Integer)
    created_at = Column(DateTime)  # '2024-06-26T10:15:44.620796'
    updated_at = Column(DateTime)  # '2024-06-26T10:15:44.620796'
    __table_args__ = (
        UniqueConstraint('id', name='_categories_uc'),
    )

    label = relationship("CategoryLabel", back_populates="category")
    session = relationship("SessionCategory", back_populates="category")


class Label(Base):  # Base Dictionary
    """
    The Label class represents the 'labels' table in the database.
    Labels used for easiness of managing and finding all categories/tags related to same label.
    It contains details about each label such as the text of the label.
    """
    __tablename__ = 'labels'
    id = Column(Integer, primary_key=True)
    text = Column(Unicode)
    # color - not interested in this data for DWH
    __table_args__ = (
        UniqueConstraint('id', name='_labels_uc'),
    )

    # Relationship
    category = relationship("CategoryLabel", back_populates="label")
    tag = relationship("TagLabel", back_populates="label")


class CategoryLabel(Base):  # Base Dictionary
    """
    The CategoryLabel class represents the 'category_labels' table in the database.
    It contains the associations between categories and labels
        for easiness of managing and finding all categories related to same label.
    """
    __tablename__ = 'category_labels'
    id = Column(Integer, primary_key=True, autoincrement=True)
    category_id = Column(Integer, ForeignKey('categories.id'))
    label_id = Column(Integer, ForeignKey('labels.id'))
    __table_args__ = (
        UniqueConstraint('category_id', 'label_id', name='_category_labels_uc'),
    )

    category = relationship("Category", back_populates="label")
    label = relationship("Label", back_populates="category")


class ScorecardCategory(Base):  # Base Dictionary
    """
    The ScorecardCategory class represents the 'scorecard_categories' table in the database.
    Each scorecard grouped into Categories, which contains Points,
        to calculate scores on grouped Category level.
    ScorecardCategory contains details about each scorecard category such as name,
        associated scorecard, and sort order.
    """
    __tablename__ = 'scorecard_categories'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    scorecard_id = Column(Integer, ForeignKey('scorecards.id'))
    sort_order = Column(Integer)
    __table_args__ = (
        UniqueConstraint('id', 'scorecard_id', name='_scorecard_categories_uc'),
    )

    # Relationships
    scorecard = relationship("Scorecard", back_populates="category")
    point = relationship("ScorecardPoint", back_populates="category")


class ScorecardPoint(Base):  # Base Dictionary
    """
    The ScorecardPoint class represents the 'scorecard_points' table in the database.
    Each scorecard grouped into Categories, which contains Points,
        to calculate scores on grouped Category level.
    It contains details about each scorecard point such as name, description,
        associated scorecard and category, etc.
    """
    __tablename__ = 'scorecard_points'
    id = Column(Integer, primary_key=True)
    scorecard_id = Column(Integer, ForeignKey('scorecards.id'))
    category_id = Column(Integer, ForeignKey('scorecard_categories.id'))
    name = Column(Unicode)
    description = Column(Unicode)
    sort_order = Column(Integer)
    critical = Column(Boolean)
    max_score = Column(Integer)
    allow_partial_score = Column(Boolean)
    # score_values  # [1] - not interested in this data for DWH
    # user_data - not interested in this data for DWH
    __table_args__ = (
        UniqueConstraint('id', 'scorecard_id', name='_scorecard_points_uc'),
    )

    # Relationship
    scorecard = relationship("Scorecard", back_populates="point")
    category = relationship("ScorecardCategory", back_populates="point")
    session_score = relationship("SessionScore", back_populates="scorecard_point")


class Tag(Base):  # Base Dictionary
    """
    The Tag class represents the 'tags' table in the database.
    Tags used to mark session contained specific (ca be setup) phrases and words in transcript.
    It contains details about each tag such as name, type, team association, etc.
    """
    __tablename__ = 'tags'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    type = Column(Unicode)
    team_id = Column(Integer, ForeignKey('groups.id'))
    is_archived = Column(Boolean)
    archived_by_id = Column(Integer)
    archived_at = Column(DateTime)  # '2024-06-26T10:15:44.620796'
    # words - not interested in this data for DWH
    # phrases - not interested in this data for DWH
    # color - not interested in this data for DWH
    __table_args__ = (
        UniqueConstraint('id', name='_tags_uc'),
    )
    group = relationship("Group", back_populates="tag")
    label = relationship("TagLabel", back_populates="tag")
    session = relationship("SessionTag", back_populates="tag")


class TagLabel(Base):  # Base Dictionary
    """
    The TagLabel class represents the 'tag_labels' table in the database.
    It contains the associations between tags and labels.
        for easiness of managing and finding all categories related to same label.
    """
    __tablename__ = 'tag_labels'
    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_id = Column(Integer, ForeignKey('tags.id'))
    label_id = Column(Integer, ForeignKey('labels.id'))
    __table_args__ = (
        UniqueConstraint('tag_id', 'label_id', name='_tag_labels_uc'),
    )

    tag = relationship("Tag", back_populates="label")
    label = relationship("Label", back_populates="tag")


# --- Data - data, changing heavily during a day: conversations, tags, scores, etc ---
class Session(Base):
    """
    The Session class represents the 'sessions' table in the database.
    Session is record about any type of conversation (call, chat, email, ticket, etc).
    It contains details about each session such as the session ID, type, caller ID, source, etc.
    """
    __tablename__ = 'sessions'
    id = Column(UNIQUEIDENTIFIER, primary_key=True)  # 39094809-18df-4bd0-aa6e-9aced976d823
    type = Column(Unicode)
    caller_id = Column(Unicode)
    source = Column(Unicode)
    language_code = Column(Unicode)
    asr_size = Column(Unicode)
    filename = Column(Unicode)
    destination_id = Column(Unicode)
    start_dt = Column(DateTime)  # '2024-06-26T10:15:44'
    # end_dt = Column(DateTime)  # '2024-06-26T10:15:44.620796' - not interested in this data for DWH
    # created_at = Column(DateTime)  # '2024-06-26T10:15:44.620796' - not interested in this data for DWH
    # updated_at = Column(DateTime)  # '2024-06-26T10:15:44.620796' - not interested in this data for DWH
    direction = Column(Unicode)
    agent_id = Column(Integer, ForeignKey('agents.id'))
    group_id = Column(Integer, ForeignKey('groups.id'))
    duration = Column(Float)
    silence = Column(Float)
    silence_percent = Column(Float)
    agent_channel = Column(Integer)
    comments_count = Column(Integer)
    default_scorecard_id = Column(Integer)
    average_score = Column(Float)
    is_processed = Column(Boolean)
    overlaps_data = Column(JSON)  # {'client': 3.12, 'agent': 1.36},
    duration_details = Column(JSON)  # {'0': 33.52, '1': 37.67999999999999},
    score_details = Column(JSON)  # {'automated_score': 0.9333333333333333, 'manual_score': 1.0},
    queue_name = Column(Unicode)
    campaign_name = Column(Unicode)
    term_reason = Column(Unicode)
    waiting_time = Column(Integer)
    fcr = Column(Integer)
    csi = Column(Integer)
    nps = Column(Integer)
    list_id = Column(Integer)
    words_count_agent = Column(Integer)
    words_count_client = Column(Integer)
    words_count_both = Column(Integer)
    caller_prev_session_id = Column(UNIQUEIDENTIFIER)  # 39094809-18df-4bd0-aa6e-9aced976d823
    additional_info = Column(JSON)  # "additional_info": {"words_count": [12, 82]} # [agent, client]
    # emotions - not interested in this data for DWH
    # sentiments - not interested in this data for DWH
    # compliance_matches = [] - not interested in this data for DWH
    # activity - not interested in this data for DWH
    __table_args__ = (
        UniqueConstraint('id', name='_sessions_uc'),
    )

    agent = relationship("Agent", back_populates="session")
    group = relationship("Group", back_populates="session")
    category = relationship("SessionCategory", back_populates="session")
    crm_status = relationship("SessionCRMStatus", back_populates="session")
    session_reviewer = relationship("SessionReviewer", back_populates="session")
    session_score = relationship("SessionScore", back_populates="session")
    tag = relationship("SessionTag", back_populates="session")
    comment = relationship("SessionComment", back_populates="session")
    summary = relationship("SessionSummary", back_populates="session")


class SessionCategory(Base):
    """
    The SessionCategory class represents the 'sessions_categories' table in the database.
    It contains the associations between sessions and categories to represent topics and categories
        for each session.
    There are possibility that single session associated with multiple Categories as customer can
        discuss multiple topics in one conversation.
    """
    __tablename__ = 'sessions_categories'
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(UNIQUEIDENTIFIER, ForeignKey('sessions.id'))  # 39094809-18df-4bd0-aa6e-9aced976d823
    category_id = Column(Integer, ForeignKey('categories.id'))
    is_verified = Column(Boolean)
    __table_args__ = (
        UniqueConstraint('session_id', 'category_id', 'is_verified', name='_sessions_categories_uc'),
    )

    category = relationship("Category", back_populates="session")
    session = relationship("Session", back_populates="category")


class SessionCRMStatus(Base):
    """
    The SessionCRMStatus class represents the 'sessions_crm_statuses' table in the database.
    It contains the CRM status information associated with each session.
    CRM statuses can be any external statuses related to company flow and state.
    """
    __tablename__ = 'sessions_crm_statuses'
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(UNIQUEIDENTIFIER, ForeignKey('sessions.id'))  # 39094809-18df-4bd0-aa6e-9aced976d823
    crm_status = Column(Unicode(255))
    __table_args__ = (
        UniqueConstraint('session_id', 'crm_status', name='_sessions_crm_statuses_uc'),
    )

    session = relationship("Session", back_populates="crm_status")


class SessionReviewer(Base):
    """
    The SessionReviewer class represents the 'sessions_reviewers' table in the database.
    It contains the associations between sessions and reviewers.
    As only user can review session, hence 'reviewer_id' is same as 'user_id'
    """
    __tablename__ = 'sessions_reviewers'
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(UNIQUEIDENTIFIER, ForeignKey('sessions.id'))  # 39094809-18df-4bd0-aa6e-9aced976d823
    reviewer_id = Column(Integer, ForeignKey('users.id'))
    last_reviewed_at = Column(DateTime)  # '2024-06-26T10:15:44.620796'
    __table_args__ = (
        UniqueConstraint('session_id', 'reviewer_id', name='_sessions_reviewers_uc'),
    )

    session = relationship("Session", back_populates="session_reviewer")
    reviewer = relationship("User", back_populates="session_reviewer")


class SessionScore(Base):
    """
    The SessionScore class represents the 'sessions_scores' table in the database.
    This is most detailed information about each quality review for each session.
    It contains the score information associated with each session, including the session ID,
        scorecard ID, and reviewer ID.
    As only user can review session, hence 'reviewer_id' is same as 'user_id'
    """
    __tablename__ = 'sessions_scores'
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(UNIQUEIDENTIFIER, ForeignKey('sessions.id'))  # 39094809-18df-4bd0-aa6e-9aced976d823
    scorecard_id = Column(Integer, ForeignKey('scorecards.id'))
    reviewer_id = Column(Integer, ForeignKey('users.id'))
    scorecard_point_id = Column(Integer, ForeignKey('scorecard_points.id'))
    score = Column(Integer)
    comment = Column(Unicode)
    # meta = Column(JSON)  # {'dispute_pending': False, 'is_agreed': False},
    __table_args__ = (
        UniqueConstraint(
            'session_id', 'scorecard_id', 'reviewer_id', 'scorecard_point_id', name='_sessions_scores_uc'
        ),
    )

    session = relationship("Session", back_populates="session_score")
    scorecard = relationship("Scorecard", back_populates="session_score")
    reviewer = relationship("User", back_populates="session_score")
    scorecard_point = relationship("ScorecardPoint", back_populates="session_score")


class SessionTag(Base):
    """
    The SessionTag class represents the 'sessions_tags' table in the database.
    It contains the associations between sessions and tags.
    """
    __tablename__ = 'sessions_tags'
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(UNIQUEIDENTIFIER, ForeignKey('sessions.id'), index=True)  # 39094809-18df-4bd0-aa6e-9aced976d823
    tag_id = Column(Integer, ForeignKey('tags.id'), index=True)
    score = Column(Float)
    matched_corpus_text = Column(Unicode)
    is_agent = Column(Boolean)
    transcript_id = Column(Integer, index=True)
    matched_query_text = Column(Unicode)
    meta = Column(JSON)
    __table_args__ = (
        UniqueConstraint('session_id', 'tag_id', "transcript_id", name='_sessions_tags_uc'),
    )

    session = relationship("Session", back_populates="tag")
    tag = relationship("Tag", back_populates="session")


class SessionComment(Base):  # Not Implemented
    __tablename__ = 'sessions_comments'
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(UNIQUEIDENTIFIER, ForeignKey('sessions.id'))  # 39094809-18df-4bd0-aa6e-9aced976d823
    author_id = Column(Integer, ForeignKey('users.id'))
    text = Column(Unicode)
    comments = Column(Unicode)
    __table_args__ = (
        UniqueConstraint('session_id', name='_sessions_comments_uc'),
    )

    session = relationship("Session", back_populates="comment")
    comment_author = relationship("User", back_populates="session_comment")


class SessionSummary(Base):  # Not Implemented
    __tablename__ = 'sessions_summaries'
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(UNIQUEIDENTIFIER, ForeignKey('sessions.id'))  # 39094809-18df-4bd0-aa6e-9aced976d823
    text = Column(UnicodeText)
    __table_args__ = (
        UniqueConstraint('session_id', name='_sessions_summaries_uc'),
    )

    session = relationship("Session", back_populates="summary")

# class SessionTranscript(Base):  # Not Implemented
#     __tablename__ = 'sessions_transcripts'
#     id = Column(Integer, primary_key=True)
#     session_id = Column(UNIQUEIDENTIFIER, ForeignKey('sessions.id'))  # 39094809-18df-4bd0-aa6e-9aced976d823
